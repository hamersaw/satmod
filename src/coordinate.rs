use failure::ResultExt;
use gdal::spatial_ref::CoordTransform;

use std::error::Error;

const GEOHASH_BOUNDS: (f64, f64, f64, f64) = (-180.0, 180.0, -90.0, 90.0);
static GEOHASH32_CHARS: &'static [char] = &['0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j',
    'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
];

const QUADTILE_BOUNDS: (f64, f64, f64, f64) = (-20037508.342789248,
    20037508.342789248, -20037508.342789248, 20037508.342789248);
static QUADTILE_CHARS: &'static [char] = &['3', '1', '2', '0'];

#[derive(Clone, Copy, Debug)]
pub enum Geocode {
    Geohash,
    QuadTile,
}

impl Geocode {
    pub fn get_code(&self, x: f64, y: f64, precision: usize)
            -> Result<String, Box<dyn Error>> {
        // retreive geocode specific parameters
        let (mut min_x, mut max_x, mut min_y, mut max_y,
                char_bits, codes) = match self {
            Geocode::Geohash => (GEOHASH_BOUNDS.0, GEOHASH_BOUNDS.1,
                GEOHASH_BOUNDS.2, GEOHASH_BOUNDS.3, 5, GEOHASH32_CHARS),
            Geocode::QuadTile => (QUADTILE_BOUNDS.0, QUADTILE_BOUNDS.1,
                QUADTILE_BOUNDS.2, QUADTILE_BOUNDS.3, 2, QUADTILE_CHARS),
        };

        // check if coordinates are valid
        if x < min_x || x > max_x || y < min_y || y > max_y {
            return Err(format!("coordinate ({}, {}) is outside of geocode range ({} - {}, {} - {})", x, y, min_x, max_x, min_y, max_y).into());
        }

        // initailize instance variables
        let mut bits_total: i8 = 0;
        let mut hash_value: usize = 0;
        let mut out = String::with_capacity(precision);

        // compute geocode code
        while out.len() < precision {
            for _ in 0..char_bits {
                if bits_total % 2 == 0 {
                    // split on x value
                    let mid = (max_x + min_x) / 2f64;
                    if x > mid {
                        hash_value = (hash_value << 1) + 1usize;
                        min_x = mid;
                    } else {
                        hash_value <<= 1;
                        max_x = mid;
                    }
                } else {
                    // split on y value
                    let mid = (max_y + min_y) / 2f64;
                    if y > mid {
                        hash_value = (hash_value << 1) + 1usize;
                        min_y = mid;
                    } else {
                        hash_value <<= 1;
                        max_y = mid;
                    }
                }
                bits_total += 1;
            }

            // append character to output
            let code: char = codes[hash_value];
            out.push(code);
            hash_value = 0;
        }

        Ok(out)
    }

    pub fn get_epsg_code(&self) -> u32 {
        match self {
            Geocode::Geohash => 4326,
            Geocode::QuadTile => 3857,
        }
    }

    pub fn get_intervals(&self, precision: usize) -> (f64, f64) {
        match self {
            Geocode::Geohash => {
                // calculate number of bits for latitude and longitude
                let lat_bits = (2 * precision) as f64
                    + (precision as f64 / 2.0).floor();
                let long_bits = (2 * precision) as f64
                    + (precision as f64 / 2.0).ceil();

                // calculate deltas
                let lat_delta = (GEOHASH_BOUNDS.3 - GEOHASH_BOUNDS.2) /
                    2_u32.pow(lat_bits as u32) as f64;
                let long_delta = (GEOHASH_BOUNDS.1 - GEOHASH_BOUNDS.0) /
                    2_u32.pow(long_bits as u32) as f64;

                (long_delta, lat_delta)
            },
            Geocode::QuadTile => {
                // calculate delta
                let delta = (QUADTILE_BOUNDS.1 - QUADTILE_BOUNDS.0) /
                    2_u32.pow(precision as u32) as f64;

                (delta, delta)
            },
        }
    }
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

pub fn transform_pixel(x: isize, y: isize, z: isize,
        transform: &[f64; 6], coord_transform: &CoordTransform)
        -> Result<(f64, f64, f64), Box<dyn Error>> {
    let x_coord = transform[0] + (x as f64 * transform[1])
        + (y as f64 * transform[2]);
    let y_coord = transform[3] + (x as f64 * transform[4])
        + (y as f64 * transform[5]);

    transform_coord(x_coord, y_coord, z as f64, coord_transform)
}

pub fn transform_pixels(pixels: &Vec<(isize, isize, isize)>,
        transform: &[f64; 6], coord_transform: &CoordTransform)
        -> Result<(Vec<f64>, Vec<f64>, Vec<f64>), Box<dyn Error>> {
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
    coord_transform.transform_coords(&mut xs, &mut ys, &mut zs).compat()?;

    Ok((xs, ys, zs))
}

pub fn transform_coord(x: f64, y: f64, z: f64,
        coord_transform: &CoordTransform)
        -> Result<(f64, f64, f64), Box<dyn Error>> {
    // insert items into buffer
    let mut xs = vec!(x);
    let mut ys = vec!(y);
    let mut zs = vec!(z);

    // transfrom coordinates
    coord_transform.transform_coords(&mut xs, &mut ys, &mut zs).compat()?;

    // return values
    Ok((xs[0], ys[0], zs[0]))
}

#[cfg(test)]
mod tests {
    use gdal::raster::Dataset;
    use gdal::spatial_ref::{CoordTransform, SpatialRef};

    use std::path::Path;

    /*#[test]
    fn get_geohash_intervals() {
        println!("{:?}", super::Geocode::Geohash
            .get_code(-105.0208241, 40.5860239, 5));
    }*/

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
        let (width, height) = (width as isize, height as isize);
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
        assert_eq!(super::Geocode::Geohash.get_intervals(1),
            (45.0, 45.0));
        assert_eq!(super::Geocode::Geohash.get_intervals(2),
            (11.25, 5.625));
        assert_eq!(super::Geocode::Geohash.get_intervals(3),
            (1.40625, 1.40625));
        assert_eq!(super::Geocode::Geohash.get_intervals(4),
            (0.3515625, 0.17578125));
        assert_eq!(super::Geocode::Geohash.get_intervals(5),
            (0.0439453125, 0.0439453125));
        assert_eq!(super::Geocode::Geohash.get_intervals(6),
            (0.010986328125, 0.0054931640625));
    }

    // TODO - test get_window_bounds
}
