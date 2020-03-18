# satmod
## OVERVIEW
A utility library to split and merge satellite images.

## REFERENCES
- https://github.com/image-rs/image/tree/master/examples/scaledown
- https://docs.rs/image/0.23.1/image/struct.ImageBuffer.html
- https://docs.rs/image/0.23.1/image/struct.SubImage.html

## SCRATCH PAD
#### image handling
    // load image
    let image = RawImage::new(path, min_lat, max_lat,
        min_long, max_long, timstamp);

    // split image into length 4 geohashes
    for st_image in image.split(precision) {
        // check if st_image covers a full geohash
        if st_image.geohash_coverage() != 1.0 {
            continue;
        }

        // send image elsewhere
    }

#### image handling - no good
    let image_boundaries = geohash::split(lat_min, lat_max,
        long_min, long_max, precision);
    for image_slice in image_slices {
        // check if image contains part of a geohash
        if geohash.coverage() < 1.0 {
            continue;
        }

        // convert bounds to pixels
        let (x_min, x_max, y_min, y_max) = geohash.to_pixels
            image.get_x_dimension(), image.get_y_dimension());

        // split image
        let subimage = image::capture(image, x_min, x_max, y_min, y_max);
    }

## TODO
- everything
