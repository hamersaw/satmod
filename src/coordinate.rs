use gdal::errors::Error;
use gdal::spatial_ref::CoordTransform;

pub fn get_geohash_intervals(precision: usize) -> (f64, f64) {
    // calculate number of bits for latitude and longitude
    let lat_bits = (2 * precision) as f64 + (precision as f64 / 2.0).floor();
    let long_bits = (2 * precision) as f64 + (precision as f64 / 2.0).ceil();

    // calculate deltas
    let lat_delta = 180.0 / 2_u32.pow(lat_bits as u32) as f64;
    let long_delta = 360.0 / 2_u32.pow(long_bits as u32) as f64;

    (lat_delta, long_delta)
}

pub fn get_window_bounds(min_x: f64, max_x: f64, min_y: f64, max_y: f64,
        x_interval: f64, y_interval: f64) -> Vec<(f64, f64, f64, f64)> {
    // compute indices for minimum and maximum coordinates
    let min_x_index = (min_x / x_interval).floor() as i32;
    let max_x_index = (max_x / x_interval).ceil() as i32;

    let min_y_index = (min_y / y_interval).floor() as i32;
    let max_y_index = (max_y / y_interval).ceil() as i32;

    // compute all window bounds
    let mut window_bounds = Vec::new();
    for x_index in min_x_index..max_x_index {
        let x_index = x_index as f64;

        for y_index in min_y_index..max_y_index {
            let y_index = y_index as f64;

            // compute window x and y bounds
            let window_x_min = x_index * x_interval;
            let window_x_max = (x_index + 1.0) * x_interval;

            let window_y_min = y_index * y_interval;
            let window_y_max = (y_index + 1.0) * y_interval;

            // add to window bounds
            window_bounds.push((window_x_min,
                window_x_max, window_y_min, window_y_max));
        }
    }

    window_bounds
}

pub fn transform_pixel(x: f64, y: f64, z: f64,
        transform: &[f64; 6], coord_transform: &CoordTransform)
        -> Result<(f64, f64, f64), Error> {
    let x_coord = transform[0] + (x * transform[1])
        + (y * transform[2]);
    let y_coord = transform[3] + (x * transform[4])
        + (y * transform[5]);

    transform_coord(x_coord, y_coord, z, coord_transform)
}

pub fn transform_pixels(pixels: &Vec<(usize, usize, usize)>,
        transform: &[f64; 6], coord_transform: &CoordTransform)
        -> Result<(Vec<f64>, Vec<f64>, Vec<f64>), Error> {
    // convert pixels to coordinates
    let mut xs: Vec<f64> = pixels.iter().map(|(x, y, _)| {
        transform[0] + (*x as f64 * transform[1])
            + (*y as f64 * transform[2])
    }).collect();

    let mut ys: Vec<f64> = pixels.iter().map(|(x, y, _)| {
        transform[3] + (*x as f64 * transform[4])
            + (*y as f64 * transform[5])
    }).collect();

    let mut zs: Vec<f64> = pixels.iter()
        .map(|(_, _, z)| *z as f64).collect();

    // perform coordinate transform
    coord_transform.transform_coords(&mut xs, &mut ys, &mut zs)?;

    Ok((xs, ys, zs))
}

pub fn transform_coord(x: f64, y: f64, z: f64,
        coord_transform: &CoordTransform) -> Result<(f64, f64, f64), Error> {
    // insert items into buffer
    let mut xs = vec!(x);
    let mut ys = vec!(y);
    let mut zs = vec!(z);

    // transfrom coordinates
    coord_transform.transform_coords(&mut xs, &mut ys, &mut zs)?;

    // return values
    Ok((xs[0], ys[0], zs[0]))
}

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};
    use gdal::spatial_ref::{CoordTransform, SpatialRef};

    use std::path::Path;

    /*#[test]
    fn transform_pixel() {
        // read dataset
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        let dataset = Dataset::open(path).expect("dataset open");

        // initialize transform and CoordTransform
        let transform = dataset.geo_transform().expect("get transform");

        let src_spatial_ref = SpatialRef::from_wkt(&dataset.projection())
            .expect("source spatial reference");
        let dst_spatial_ref = SpatialRef::from_epsg(4326)
            .expect("destination spatial reference");
        let coord_transform = CoordTransform::new(&src_spatial_ref,
            &dst_spatial_ref).expect("coordinate transform");

        // transform corner pixels
        let (width, height) = dataset.size();
        assert_eq!(super::transform_pixel(0, 0, 0, &transform,
                &coord_transform).expect("ul pixel transform"),
            (-106.1831726065988, 40.644794803779625, 0.0));

        assert_eq!(super::transform_pixel(0, height, 0, &transform,
                &coord_transform).expect("ll pixel transform"),
            (-106.16613169554964, 39.65575257223607, 0.0));

        assert_eq!(super::transform_pixel(width, 0, 0, &transform,
                &coord_transform).expect("ur pixel transform"),
            (-104.88455693238069, 40.65079881091997, 0.0));

        assert_eq!(super::transform_pixel(width, height, 0, &transform,
                &coord_transform).expect("lr pixel transform"),
            (-104.8862200854741, 39.66155122695049, 0.0));
    }*/

    #[test]
    fn get_geohash_intervals() {
        assert_eq!(super::get_geohash_intervals(1),
            (45.0, 45.0));
        assert_eq!(super::get_geohash_intervals(2),
            (5.625, 11.25));
        assert_eq!(super::get_geohash_intervals(3),
            (1.40625, 1.40625));
        assert_eq!(super::get_geohash_intervals(4),
            (0.17578125, 0.3515625));
        assert_eq!(super::get_geohash_intervals(5),
            (0.0439453125, 0.0439453125));
        assert_eq!(super::get_geohash_intervals(6),
            (0.0054931640625, 0.010986328125));
    }

    // TODO - test get_window_bounds
}
