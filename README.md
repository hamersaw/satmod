# st-image
## OVERVIEW
A utility library to manage spatiotemporal images.

## REFERENCES
- https://github.com/image-rs/image/tree/master/examples/scaledown
- https://docs.rs/image/0.23.1/image/struct.ImageBuffer.html
- https://docs.rs/image/0.23.1/image/struct.SubImage.html

## SCRATCH PAD
#### image handling
    // load image
    let image = RawImage::new(path, min_lat, max_lat,
        min_long, max_long, timstamp);

    // split image into length 'precision' geohashes
    for st_image in image.split(precision) {
        // check if st_image covers a full geohash
        if st_image.geohash_coverage() != 1.0 {
            continue;
        }

        // send image elsewhere
    }

## TODO
- everything
