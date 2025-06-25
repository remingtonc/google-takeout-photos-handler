package main

import (
	"crypto/sha1"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
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
	useChecksum           bool
	detectDuplicates      bool
	skipDuplicates        bool
	inputDir              string
	outputDir             string
	maxDirWorkers         int
	maxFileWorkers        int
	totalFilesProcessed   int64
	totalMetadataRepaired int64

	metaTracking      = make(map[string]string)
	metaTrackingMutex sync.Mutex

	fileIndex      []IndexedFile
	duplicateIndex = make(map[string][]IndexedFile)
	skippedPaths   = make(map[string]bool)
	symlink        bool
	dangerFixExif  bool
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
	flag.BoolVar(&useChecksum, "use-checksum", false, "Use checksums when comparing duplicate files")
	flag.BoolVar(&detectDuplicates, "detect-duplicates", true, "Detect and group duplicate files")
	flag.BoolVar(&skipDuplicates, "skip-duplicates", false, "Skip processing files detected as duplicates")
	flag.BoolVar(&symlink, "symlink", false, "Create symlinks instead of copying files")
	flag.BoolVar(&dangerFixExif, "danger-fix-exif", false, "Attempt to fix EXIF data when corrupted")
	flag.Parse()
}

var exifTool *exiftool.Exiftool

type IndexedFile struct {
	Path       string
	FileName   string
	Album      string
	Size       int64
	Checksum   string
	Timestamp  string
	MetaSource string
}

func main() {
	var startTime = time.Now()
	if detectDuplicates {
		scanAllFiles()
	}
	dirs, err := os.ReadDir(inputDir)
	if err != nil {
		log.Fatalf("Input directory cannot be read: %v", err)
	}

	_, err = os.Stat(outputDir)
	if os.IsNotExist(err) {
		log.Fatalf("Output directory does not exist: %v", err)
	}

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

	log.Printf("Processing directories now with %d directory workers and %d file workers", maxDirWorkers, maxFileWorkers)
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

	log.Printf("Processed directories. Total files processed: %d, metadata repaired: %d", totalFilesProcessed, totalMetadataRepaired)
	log.Printf("Writing metadata...")
	err = writeMetadataMap()
	if err != nil {
		log.Fatalf("Error writing metadata map: %v", err)
	}
	err = writeDuplicateReport()
	if err != nil {
		log.Fatalf("Error writing duplicate report: %v", err)
	}
	log.Printf("Wrote metadata.")
	log.Printf("Total time: %s", time.Since(startTime))
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

func writeDuplicateReport() error {
	if dryRun {
		return nil
	}
	if len(duplicateIndex) == 0 {
		return nil
	}
	report := make(map[string][]string)
	for key, files := range duplicateIndex {
		if len(files) > 1 {
			paths := make([]string, 0, len(files))
			for _, f := range files {
				paths = append(paths, f.Path)
			}
			report[key] = paths
		}
	}
	if len(report) == 0 {
		return nil
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal duplicate report: %v", err)
	}
	return os.WriteFile(filepath.Join(outputDir, "duplicates.json"), data, 0644)
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
	if skipDuplicates {
		if skippedPaths[filePath] {
			log.Printf("Skipping duplicate file: %s", filePath)
			return filepath.Base(filePath), false, nil
		}
	}
	base := filepath.Base(filePath)
	outPath := filepath.Join(outputDir, base)

	repaired := false
	metaSource := "none"
	var meta *Metadata
	var err error

	if metadataPath != "" {
		meta, err = findSupplementalMetadata(filePath)
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
		err = applyExif(outPath, meta, false)
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

func findSupplementalMetadata(filePath string) (*Metadata, error) {
	dir := filepath.Dir(filePath)
	base := filepath.Base(filePath)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	bestMatch := ""
	maxMatchLen := 0
	name := strings.TrimSuffix(base, filepath.Ext(base))
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		metaBase := strings.TrimSuffix(f.Name(), filepath.Ext(f.Name()))
		if strings.HasPrefix(metaBase, name) {
			if len(name) > maxMatchLen {
				bestMatch = filepath.Join(dir, f.Name())
				maxMatchLen = len(name)
			}
		} else if strings.Contains(name, metaBase) || strings.Contains(metaBase, name) {
			if len(metaBase) > maxMatchLen {
				bestMatch = filepath.Join(dir, f.Name())
				maxMatchLen = len(metaBase)
			}
		}
	}
	if bestMatch == "" {
		return nil, fmt.Errorf("no matching metadata for %s", base)
	}
	return parseMetadata(bestMatch)
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

func applyExif(path string, meta *Metadata, fixAttempted bool) error {
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
			if dangerFixExif && !fixAttempted {
				log.Printf("Blindly attempting to fix EXIF for %s before error.", path)
				// https://exiftool.org/faq.html#Q20
				cmd := exec.Command("exiftool", "-all=", "-tagsfromfile", "@", "-all:all", "-unsafe", "-icc_profile", path)
				if err := cmd.Run(); err != nil {
					return err
				}
				return applyExif(path, meta, true)
			}
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
	if dryRun {
		return nil
	}
	if symlink {
		return os.Symlink(src, dst)
	} else {
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
		"20060102",            // WP_20150313_009
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

func scanAllFiles() {
	log.Println("Scanning all files for duplicate detection...")
	err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || strings.HasSuffix(path, ".json") {
			return nil
		}
		album := extractAlbumFromPath(path)
		size := info.Size()
		timestamp := ""
		metaSource := ""
		if meta, err := findSupplementalMetadata(path); err == nil {
			timestamp = meta.PhotoTakenTime.Timestamp
			metaSource = "supplemental"
		} else if useFilenameDate {
			if guessed := guessMetadataFromFilename(filepath.Base(path)); guessed != nil {
				timestamp = guessed.PhotoTakenTime.Timestamp
				metaSource = "filename"
			}
		} else if useAlbumYear {
			albumYear := extractYearFromPath(path)
			if albumYear != 0 {
				t := time.Date(albumYear, 1, 1, 0, 0, 0, 0, time.UTC)
				timestamp = fmt.Sprintf("%d", t.Unix())
				metaSource = "album-year"
			}
		}
		checksum := ""
		if useChecksum {
			if f, err := os.Open(path); err == nil {
				h := sha1.New()
				if _, err := io.Copy(h, f); err == nil {
					checksum = fmt.Sprintf("%x", h.Sum(nil))
				}
				f.Close()
			}
		}
		var key string
		if useChecksum {
			key = checksum
		} else {
			key = fmt.Sprintf("%d|%s", size, timestamp)
		}
		file := IndexedFile{
			Path: path, FileName: filepath.Base(path), Album: album,
			Size: size, Checksum: checksum, Timestamp: timestamp, MetaSource: metaSource,
		}
		fileIndex = append(fileIndex, file)
		duplicateIndex[key] = append(duplicateIndex[key], file)
		return nil
	})
	if err != nil {
		log.Fatalf("Error scanning files: %v", err)
	}
	log.Printf("Scanned %d files. Identified %d duplicate groups.", len(fileIndex), countDuplicateGroups())
}

func countDuplicateGroups() int {
	count := 0
	for _, files := range duplicateIndex {
		if len(files) > 1 {
			count++
			for idx, f := range files {
				if idx == 0 {
					continue // keep the first occurrence
				}
				skippedPaths[f.Path] = true
			}
		}
	}
	return count
}

func extractAlbumFromPath(path string) string {
	parts := strings.Split(path, string(os.PathSeparator))
	for i := len(parts) - 1; i >= 0; i-- {
		if strings.HasPrefix(parts[i], "Photos from ") {
			return parts[i]
		}
	}
	return "unknown"
}

func writeMetadataMap() error {
	if dryRun {
		return nil
	}
	metaTrackingMutex.Lock()
	defer metaTrackingMutex.Unlock()
	out, err := json.MarshalIndent(metaTracking, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata map: %v", err)
	}
	return os.WriteFile(filepath.Join(outputDir, "metadata_map.json"), out, 0644)
}
