use gdal::errors::Error;
use gdal::spatial_ref::{CoordTransform, SpatialRef};

pub struct StCoordTransform {
    coord_transform: CoordTransform,
    transform: [f64; 6],
}

impl StCoordTransform {
    pub fn new(transform: [f64; 6], src_projection: &str,
            dst_projection: u32) -> Result<StCoordTransform, Error> {
        let src_spatial_ref = SpatialRef::from_wkt(src_projection)?;
        let dst_spatial_ref = SpatialRef::from_epsg(dst_projection)?;
        let coord_transform = 
            CoordTransform::new(&src_spatial_ref, &dst_spatial_ref)?;

        Ok(StCoordTransform {
            coord_transform: coord_transform,
            transform: transform,
        })
    }

    pub fn transform_pixel(&self, x: usize, y: usize, z: usize)
            -> Result<(f64, f64, f64), Error> {
        let x_coord = self.transform[0] + (x as f64 * self.transform[1])
            + (y as f64 * self.transform[2]);
        let y_coord = self.transform[3] + (x as f64 * self.transform[4])
            + (y as f64 * self.transform[5]);

        self.transform_coord(x_coord, y_coord, z as f64)
    }

    pub fn transform_pixels(&self, pixels: &Vec<(usize, usize, usize)>)
            -> Result<(Vec<f64>, Vec<f64>, Vec<f64>), Error> {
        // convert pixels to coordinates
        let mut xs: Vec<f64> = pixels.iter().map(|(x, y, _)| {
            self.transform[0] + (*x as f64 * self.transform[1])
                + (*y as f64 * self.transform[2])
        }).collect();

        let mut ys: Vec<f64> = pixels.iter().map(|(x, y, _)| {
            self.transform[3] + (*x as f64 * self.transform[4])
                + (*y as f64 * self.transform[5])
        }).collect();

        let mut zs = pixels.iter()
            .map(|(_, _, z)| *z as f64).collect();

        // perform coordinate transform
        self.transform_coords(&mut xs, &mut ys, &mut zs)?;

        Ok((xs, ys, zs))
    }

    pub fn transform_coord(&self, x: f64, y: f64, z: f64)
            -> Result<(f64, f64, f64), Error> {
        // insert items into buffer
        let mut xs = vec!(x);
        let mut ys = vec!(y);
        let mut zs = vec!(z);

        // transfrom coordinates
        self.transform_coords(&mut xs, &mut ys, &mut zs)?;

        // return values
        Ok((xs[0], ys[0], zs[0]))
    }

    pub fn transform_coords(&self, xs: &mut Vec<f64>, ys: &mut Vec<f64>,
            zs: &mut Vec<f64>) -> Result<(), Error> {
        // transfrom coordinates
        self.coord_transform.transform_coords(xs, ys, zs)
    }
}

/*fn get_coordinate_deltas(precision: usize) -> (f64, f64) {
    // calculate number of bits for latitude and longitude
    let lat_bits = (2 * precision) as f64 + (precision as f64 / 2.0).floor();
    let long_bits = (2 * precision) as f64 + (precision as f64 / 2.0).ceil();

    // calculate deltas
    let lat_delta = 180.0 / 2_u32.pow(lat_bits as u32) as f64;
    let long_delta = 360.0 / 2_u32.pow(long_bits as u32) as f64;

    (lat_delta, long_delta)
}

pub fn get_coordinate_bounds(lat_min: f64, lat_max: f64, long_min: f64,
        long_max: f64, precision: usize) -> Vec<(f64, f64, f64, f64)> {
    // calculate indices for minimum and maximum coordinates
    let (lat_delta, long_delta) = get_coordinate_deltas(precision);

    let lat_min_index = (lat_min / lat_delta).floor() as i32;
    let lat_max_index = (lat_max / lat_delta).ceil() as i32;

    let long_min_index = (long_min / long_delta).floor() as i32;
    let long_max_index = (long_max / long_delta).ceil() as i32;

    // calculate geohash bounds
    let mut coordinate_bounds = Vec::new();
    for lat_index in lat_min_index..lat_max_index {
        let lat_index = lat_index as f64;
        for long_index in long_min_index..long_max_index {
            let long_index = long_index as f64;

            // calculate subimage bounds
            let bound_lat_min = lat_index * lat_delta;
            let bound_lat_max = (lat_index + 1.0) * lat_delta;

            let bound_long_min = long_index * long_delta;
            let bound_long_max = (long_index + 1.0) * long_delta;
 
            // add to coordinate bounds
            coordinate_bounds.push((bound_lat_min, bound_lat_max,
                bound_long_min, bound_long_max));
        }
    }

    coordinate_bounds
}*/

fn get_geohash_intervals(precision: usize) -> (f64, f64) {
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

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};

    use super::StCoordTransform;

    use std::path::Path;

    #[test]
    fn transform_pixel() {
        // read dataset
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        let dataset = Dataset::open(path).expect("dataset open");

        // initialize coordinate transform
        let mut coord_transform = StCoordTransform::new(
            dataset.geo_transform().expect("get transform"),
            &dataset.projection(), 4326).expect("init StCoordTransform");

        // transform corner pixels
        let (width, height) = dataset.size();
        assert_eq!(coord_transform.transform_pixel(0, 0, 0)
                .expect("ul pixel transform"),
            (-106.1831726065988, 40.644794803779625, 0.0));

        assert_eq!(coord_transform.transform_pixel(0, height, 0)
                .expect("ll pixel transform"),
            (-106.16613169554964, 39.65575257223607, 0.0));

        assert_eq!(coord_transform.transform_pixel(width, 0, 0)
                .expect("ur pixel transform"),
            (-104.88455693238069, 40.65079881091997, 0.0));

        assert_eq!(coord_transform.transform_pixel(width, height, 0)
                .expect("lr pixel transform"),
            (-104.8862200854741, 39.66155122695049, 0.0));
    }

    /*#[test]
    fn coordinate_delta() {
        assert_eq!(super::get_coordinate_deltas(1),
            (45.0, 45.0));
        assert_eq!(super::get_coordinate_deltas(2),
            (5.625, 11.25));
        assert_eq!(super::get_coordinate_deltas(3),
            (1.40625, 1.40625));
        assert_eq!(super::get_coordinate_deltas(4),
            (0.17578125, 0.3515625));
        assert_eq!(super::get_coordinate_deltas(5),
            (0.0439453125, 0.0439453125));
        assert_eq!(super::get_coordinate_deltas(6),
            (0.0054931640625, 0.010986328125));
    }

    #[test]
    fn bounds() {
        // TODO - figure out how to unit test
        let _bounds = super::get_coordinate_bounds(-80.0, -70.0, 70.0, 80.0, 3);
        for bound in bounds {
            println!("{:?}", bound);
        }*
    }*/
}
