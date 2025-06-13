package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barasher/go-exiftool"
)

type Timestamp struct {
	Timestamp string `json:"timestamp"`
	Formatted string `json:"formatted"`
}

type Geo struct {
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	Altitude      float64 `json:"altitude"`
	LatitudeSpan  float64 `json:"latitudeSpan"`
	LongitudeSpan float64 `json:"longitudeSpan"`
}

type Metadata struct {
	Title          string    `json:"title"`
	Description    string    `json:"description"`
	CreationTime   Timestamp `json:"creationTime"`
	PhotoTakenTime Timestamp `json:"photoTakenTime"`
	GeoData        Geo       `json:"geoData"`
}

var (
	dryRun                bool
	inputDir              string
	outputDir             string
	maxDirWorkers         int
	maxFileWorkers        int
	totalFilesProcessed   int64
	totalMetadataRepaired int64

	metaTracking      = make(map[string]string)
	metaTrackingMutex sync.Mutex
)

var (
	useFilenameDate = true
	useAlbumYear    = true
)

func init() {
	flag.StringVar(&inputDir, "input", "./Takeout/Google Photos", "Input Google Photos directory")
	flag.StringVar(&outputDir, "output", "./output/photos-flat", "Flat output directory for media")
	flag.IntVar(&maxDirWorkers, "max-dir-workers", 4, "Max number of directory workers")
	flag.IntVar(&maxFileWorkers, "max-file-workers", 8, "Max number of file workers per directory")
	flag.BoolVar(&useFilenameDate, "use-filename-date", true, "Use timestamps from filenames if no supplemental metadata is present")
	flag.BoolVar(&useAlbumYear, "use-album-year", true, "Use album year (from folder name) if no other metadata is available")
	flag.BoolVar(&dryRun, "dry-run", false, "Perform a dry run without writing files")
	flag.Parse()
}

var exifTool *exiftool.Exiftool

func main() {
	dirs, err := os.ReadDir(inputDir)
	if err != nil {
		log.Fatalf("Input directory cannot be read: %v", err)
	}

	_, err = os.Stat(outputDir)
	if os.IsNotExist(err) {
		log.Fatalf("Output directory does not exist: %v", err)
	}

	log.Println("Starting processing of albums...")

	if !dryRun {
		var err error
		exifTool, err = exiftool.NewExiftool()
		if err != nil {
			log.Fatalf("Failed to initialize exiftool: %v", err)
		}
		defer exifTool.Close()
	}
	done := make(chan struct{})
	go progressLogger(done)

	dirChan := make(chan os.DirEntry, len(dirs))
	var wg sync.WaitGroup

	for range maxDirWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range dirChan {
				if dir.IsDir() {
					processAlbumDir(filepath.Join(inputDir, dir.Name()))
				}
			}
		}()
	}

	for _, d := range dirs {
		dirChan <- d
	}
	close(dirChan)
	wg.Wait()
	close(done)

	log.Printf("Processing complete. Total files processed: %d, metadata repaired: %d", totalFilesProcessed, totalMetadataRepaired)
	writeMetadataMap()
}

func progressLogger(done <-chan struct{}) {
	for {
		select {
		case <-time.After(10 * time.Second):
			log.Printf("Progress: total files processed: %d, metadata repaired: %d", atomic.LoadInt64(&totalFilesProcessed), atomic.LoadInt64(&totalMetadataRepaired))
		case <-done:
			return
		}
	}
}

func processAlbumDir(albumPath string) {
	entries, err := os.ReadDir(albumPath)
	if err != nil {
		log.Printf("failed to read album %s: %v", albumPath, err)
		return
	}

	jsonFiles := make([]os.DirEntry, 0)
	mediaFiles := make([]os.DirEntry, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".json") {
			jsonFiles = append(jsonFiles, entry)
		} else {
			mediaFiles = append(mediaFiles, entry)
		}
	}

	fileToMetadata := make(map[string]string)
	for _, media := range mediaFiles {
		bestMatch := ""
		maxMatchLen := 0
		base := strings.TrimSuffix(media.Name(), filepath.Ext(media.Name()))
		for _, meta := range jsonFiles {
			metaBase := strings.TrimSuffix(meta.Name(), filepath.Ext(meta.Name()))
			if strings.HasPrefix(metaBase, base) {
				if len(base) > maxMatchLen {
					bestMatch = filepath.Join(albumPath, meta.Name())
					maxMatchLen = len(base)
				}
			} else if strings.Contains(base, metaBase) || strings.Contains(metaBase, base) {
				if len(metaBase) > maxMatchLen {
					bestMatch = filepath.Join(albumPath, meta.Name())
					maxMatchLen = len(metaBase)
				}
			}
		}
		fileToMetadata[media.Name()] = bestMatch
	}

	sem := make(chan struct{}, maxFileWorkers)
	var wg sync.WaitGroup

	var albumFilesMu sync.Mutex
	albumFiles := []string{}
	albumMetaRepaired := int64(0)

	for _, entry := range mediaFiles {
		localEntry := entry // capture range variable
		sem <- struct{}{}
		wg.Add(1)
		go func(entry os.DirEntry) {
			defer wg.Done()
			defer func() { <-sem }()

			_, err := entry.Info()
			if err != nil {
				log.Printf("error reading file info for %s: %v", entry.Name(), err)
				return
			}
			filePath := filepath.Join(albumPath, entry.Name())
			metaPath := fileToMetadata[entry.Name()] // from preloaded map
			targetName, repaired, err := processFile(filePath, metaPath)
			if err == nil {
				albumFilesMu.Lock()
				albumFiles = append(albumFiles, targetName)
				albumFilesMu.Unlock()
				atomic.AddInt64(&totalFilesProcessed, 1)
				if repaired {
					atomic.AddInt64(&totalMetadataRepaired, 1)
					atomic.AddInt64(&albumMetaRepaired, 1)
				}
			} else {
				log.Printf("error processing file %s: %v", filePath, err)
			}
		}(localEntry)
	}

	wg.Wait()
	log.Printf("Finished album: %s — %d files, %d metadata repaired\n", filepath.Base(albumPath), len(albumFiles), albumMetaRepaired)
	writeAlbumJSON(albumPath, albumFiles)
}

func processFile(filePath, metadataPath string) (string, bool, error) {
	base := filepath.Base(filePath)
	outPath := filepath.Join(outputDir, base)

	repaired := false
	metaSource := "none"
	var meta *Metadata
	var err error

	if metadataPath != "" {
		meta, err = parseMetadata(metadataPath)
		if err != nil {
			return "", false, err
		}
		repaired = true
		metaSource = "supplemental"
	} else if useFilenameDate {
		if guessed := guessMetadataFromFilename(base); guessed != nil {
			meta = guessed
			metaSource = "filename"
		}
	}

	if meta == nil && useAlbumYear {
		albumYear := extractYearFromPath(filePath)
		if albumYear != 0 {
			metaSource = "album-year"
			t := time.Date(albumYear, 1, 1, 0, 0, 0, 0, time.UTC)
			meta = &Metadata{
				PhotoTakenTime: Timestamp{
					Timestamp: fmt.Sprintf("%d", t.Unix()),
					Formatted: t.Format("2006:01:02 15:04:05"),
				},
				CreationTime: Timestamp{
					Timestamp: fmt.Sprintf("%d", t.Unix()),
					Formatted: t.Format("Jan 2, 2006, 3:04:05 PM MST"),
				},
			}
		}
	}

	if !dryRun {
		err = copyFile(filePath, outPath)
	}

	if meta != nil && !dryRun {
		err = applyExif(outPath, meta)
		if err != nil {
			log.Printf("Failed to apply EXIF metadata for %s: %v", outPath, err)
			repaired = false
		}
		err = applyFileTimestamp(outPath, meta.PhotoTakenTime.Timestamp)
		if err != nil {
			log.Printf("Failed to apply file timestamp for %s: %v", outPath, err)
			repaired = false
		}
	}

	metaTrackingMutex.Lock()
	metaTracking[base] = metaSource
	metaTrackingMutex.Unlock()

	return base, repaired, err
}

func parseMetadata(path string) (*Metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var meta Metadata
	err = json.Unmarshal(data, &meta)
	return &meta, err
}

func applyExif(path string, meta *Metadata) error {
	if meta.PhotoTakenTime.Formatted == "" && meta.CreationTime.Formatted == "" && meta.GeoData.Latitude == 0 && meta.GeoData.Longitude == 0 {
		return fmt.Errorf("no metadata supplied for %s", path)
	}

	// reuse shared exifTool instance
	metadatas := exifTool.ExtractMetadata(path)
	if len(metadatas) > 1 {
		log.Printf("WARNING: multiple metadata entries found for %s", path)
	}
	for idx, m := range metadatas {
		if m.Err != nil {
			log.Printf("Resetting metadata due to error reading for %s: %v", path, m.Err)
			metadatas[idx] = exiftool.EmptyFileMetadata()
		}
	}
	if meta.PhotoTakenTime.Timestamp != "" {
		tsInt, err := strconv.ParseInt(meta.PhotoTakenTime.Timestamp, 10, 64)
		if err == nil {
			parsed := time.Unix(tsInt, 0)
			formatted := parsed.Format("2006:01:02 15:04:05")
			metadatas[0].SetString("DateTimeOriginal", formatted)
		} else {
			log.Printf("Invalid PhotoTakenTime timestamp for %s: %v", path, err)
		}
	}
	if meta.CreationTime.Timestamp != "" {
		tsInt, err := strconv.ParseInt(meta.CreationTime.Timestamp, 10, 64)
		if err == nil {
			parsed := time.Unix(tsInt, 0)
			formatted := parsed.Format("2006:01:02 15:04:05")
			metadatas[0].SetString("CreateDate", formatted)
		} else {
			log.Printf("Invalid CreationTime timestamp for %s: %v", path, err)
		}
	}
	if meta.GeoData.Latitude != 0 {
		metadatas[0].SetFloat("GPSLatitude", meta.GeoData.Latitude)
	}
	if meta.GeoData.Longitude != 0 {
		metadatas[0].SetFloat("GPSLongitude", meta.GeoData.Longitude)
	}

	safeApply := true
	for _, m := range metadatas {
		if m.Err != nil {
			log.Printf("WARNING: Unexpected error in metadata for %s: %v", path, m.Err)
			safeApply = false
		}
	}
	if !safeApply {
		log.Printf("Skipping EXIF write for %s due to previous errors", path)
		return fmt.Errorf("metadata contains errors, skipping write")
	}
	if !dryRun {
		exifTool.WriteMetadata(metadatas)
	} else {
		log.Printf("Dry run: would write metadata for %s", path)
	}
	for _, m := range metadatas {
		if m.Err != nil {
			return fmt.Errorf("error writing metadata for %s: %v", path, m.Err)
		}
	}
	return nil
}

func applyFileTimestamp(path, unixTimestamp string) error {
	tsInt, err := strconv.ParseInt(unixTimestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp for %s: %v", path, err)
	}
	t := time.Unix(tsInt, 0)
	if t.IsZero() {
		return fmt.Errorf("zero timestamp for %s", path)
	}
	err = os.Chtimes(path, t, t)
	if err != nil {
		return fmt.Errorf("failed to change file times for %s: %v", path, err)
	}
	return nil
}

func copyFile(src, dst string) error {
	sin, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sin.Close()

	sout, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer sout.Close()

	_, err = io.Copy(sout, sin)
	return err
}

func writeAlbumJSON(albumPath string, files []string) {
	if dryRun {
		return
	}
	albumName := filepath.Base(albumPath)
	manifest := map[string]interface{}{
		"album": albumName,
		"files": files,
	}
	out, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		log.Printf("failed to marshal album JSON: %v", err)
		return
	}
	jsonPath := filepath.Join(outputDir, albumName+".json")
	os.WriteFile(jsonPath, out, 0644)
}

func guessMetadataFromFilename(filename string) *Metadata {
	// Try to parse known datetime formats from filenames like IMG_20200101_123456.jpg
	patterns := []string{
		"20060102_150405",     // IMG_20200101_123456
		"2006-01-02-15-04-05", // 2020-01-02-15-04-05
		"20060102-150405",     // 20200102-150405
	}
	name := strings.TrimSuffix(filename, filepath.Ext(filename))
	for _, pat := range patterns {
		if len(name) >= len(pat) {
			substr := name[len(name)-len(pat):]
			if t, err := time.Parse(pat, substr); err == nil {
				ts := fmt.Sprintf("%d", t.Unix())
				formatted := t.Format("2006:01:02 15:04:05")
				return &Metadata{
					PhotoTakenTime: Timestamp{Timestamp: ts, Formatted: formatted},
					CreationTime:   Timestamp{Timestamp: ts, Formatted: formatted},
				}
			}
		}
	}
	return nil
}

func extractYearFromPath(path string) int {
	parts := strings.Split(path, string(os.PathSeparator))
	for _, part := range parts {
		if strings.HasPrefix(part, "Photos from ") {
			yearStr := strings.TrimPrefix(part, "Photos from ")
			if year, err := strconv.Atoi(yearStr); err == nil {
				return year
			}
		}
	}
	return 0
}

func writeMetadataMap() {
	if dryRun {
		return
	}
	metaTrackingMutex.Lock()
	defer metaTrackingMutex.Unlock()
	out, err := json.MarshalIndent(metaTracking, "", "  ")
	if err != nil {
		log.Printf("failed to marshal metadata map: %v", err)
		return
	}
	os.WriteFile(filepath.Join(outputDir, "metadata_map.json"), out, 0644)
}
