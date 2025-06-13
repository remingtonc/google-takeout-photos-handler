# google-takeout-photos-handler

podman run --rm -it --mount=type=bind,source=combined_takeout/Takeout/Google\ Photos/,destination=/input --mount=type=bind,source=helped_takeout/,destination=/output localhost/gtph:0.1 -input /input -output /output -dry-run