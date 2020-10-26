use gdal::Dataset;
use gdal::spatial_ref::{CoordTransform, SpatialRef};
use gdal_sys::OSRAxisMappingStrategy;

use std::error::Error;

pub type WindowBounds = (Vec<f64>, Vec<f64>, Vec<f64>);

const GEOHASH_BOUNDS: (f64, f64, f64, f64) = (-180.0, 180.0, -90.0, 90.0);
static GEOHASH32_CHARS: &[char] = &['0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j',
    'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
];

const QUADTILE_BOUNDS: (f64, f64, f64, f64) = (-20037508.342789248,
    20037508.342789248, -20037508.342789248, 20037508.342789248);
static QUADTILE_CHARS: &[char] = &['2', '0', '3', '1'];

#[derive(Clone, Copy, Debug)]
pub enum Geocode {
    Geohash,
    QuadTile,
}

impl Geocode {
    pub fn decode(&self, _value: &str)
            -> Result<(f64, f64, f64, f64), Box<dyn Error>> {
        unimplemented!(); // TODO - implement
    }

    pub fn encode(&self, x: f64, y: f64, precision: usize)
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

pub fn get_bounds(dataset: &Dataset, epsg_code: u32)
        -> Result<(f64, f64, f64, f64), Box<dyn Error>> {
    // initialize transform array and CoordTransform's from dataset
    let transform = dataset.geo_transform()?;

    let src_spatial_ref = SpatialRef::from_wkt(
        &dataset.projection())?;
    let dst_spatial_ref = SpatialRef::from_epsg(epsg_code)?;

    src_spatial_ref.set_axis_mapping_strategy(
        OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);
    dst_spatial_ref.set_axis_mapping_strategy(
        OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);

    let coord_transform = CoordTransform::new(
        &src_spatial_ref, &dst_spatial_ref)?;

    // compute minimum and maximum x and y coordinates
    let (src_width, src_height) = dataset.raster_size();
    let corner_pixels = vec![
        (0, 0, 0),
        (src_width as isize, 0, 0),
        (0, src_height as isize, 0),
        (src_width as isize, src_height as isize, 0)
    ];

    let (xs, ys, _) = transform_pixels(&corner_pixels,
        &transform, &coord_transform)?;

    let min_cx = xs.iter().cloned().fold(1./0., f64::min);
    let max_cx = xs.iter().cloned().fold(f64::NAN, f64::max);
    let min_cy = ys.iter().cloned().fold(1./0., f64::min);
    let max_cy = ys.iter().cloned().fold(f64::NAN, f64::max);

    Ok((min_cx, max_cx, min_cy, max_cy))
}

pub fn get_windows(min_x: f64, max_x: f64, min_y: f64, max_y: f64,
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

pub fn transform_pixels(pixels: &[(isize, isize, isize)],
        transform: &[f64; 6], coord_transform: &CoordTransform)
        -> Result<WindowBounds, Box<dyn Error>> {
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
        coord_transform: &CoordTransform)
        -> Result<(f64, f64, f64), Box<dyn Error>> {
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
    use super::Geocode;

    use gdal::spatial_ref::{CoordTransform, SpatialRef};
    use gdal_sys::OSRAxisMappingStrategy;

    const APPLETON_LAT_LONG: (f64, f64) = (-88.4, 44.266667);
    const APPLETON_MERCATOR: (f64, f64) = (-9840642.99, 5506802.68);
    const FORT_COLLINS_LAT_LONG: (f64, f64) = (-105.078056, 40.559167);
    const FORT_COLLINS_MERCATOR: (f64, f64) = (-11697235.69, 4947534.74);
    const HAMBURG_LAT_LONG: (f64, f64) = (10.001389, 53.565278);
    const HAMBURG_MERCATOR: (f64, f64) = (1113349.53, 7088251.30);

    #[test]
    fn geohash_encode() {
        let geocode = Geocode::Geohash;

        let result = geocode.encode(
            HAMBURG_LAT_LONG.0, HAMBURG_LAT_LONG.1, 4);
        assert!(result.is_ok());
        assert_eq!("u1x0", &result.unwrap());

        let result = geocode.encode(
            FORT_COLLINS_LAT_LONG.0, FORT_COLLINS_LAT_LONG.1, 6);
        assert!(result.is_ok());
        assert_eq!("9xjq8z", &result.unwrap());

        let result = geocode.encode(
            APPLETON_LAT_LONG.0, APPLETON_LAT_LONG.1, 8);
        assert!(result.is_ok());
        assert_eq!("dpc5u6t0", &result.unwrap());
    }

    #[test]
    fn transform_coord() {
        // initialize CoordTransform
        let geohash_code = Geocode::Geohash.get_epsg_code();
        let src_spatial_ref = SpatialRef::from_epsg(geohash_code)
            .expect("initailize geohash SpatialRef");

        let quadtile_code = Geocode::QuadTile.get_epsg_code();
        let dst_spatial_ref = SpatialRef::from_epsg(quadtile_code)
            .expect("initailize quadtile SpatialRef");
    
        src_spatial_ref.set_axis_mapping_strategy(
            OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);
        dst_spatial_ref.set_axis_mapping_strategy(
            OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);

        let coord_transform = CoordTransform::new(&src_spatial_ref,
            &dst_spatial_ref).expect("intiailize CoordTransform");

        // test coordinates
        let result = super::transform_coord(HAMBURG_LAT_LONG.0,
            HAMBURG_LAT_LONG.1, 0.0, &coord_transform);
        assert!(result.is_ok());

        let coordinates = result.unwrap();
        assert!((coordinates.0 - HAMBURG_MERCATOR.0).abs() < 0.01);
        assert!((coordinates.1 - HAMBURG_MERCATOR.1).abs() < 0.01);

        let result = super::transform_coord(FORT_COLLINS_LAT_LONG.0,
            FORT_COLLINS_LAT_LONG.1, 0.0, &coord_transform);
        assert!(result.is_ok());

        let coordinates = result.unwrap();
        assert!((coordinates.0 - FORT_COLLINS_MERCATOR.0).abs() < 0.01);
        assert!((coordinates.1 - FORT_COLLINS_MERCATOR.1).abs() < 0.01);

        let result = super::transform_coord(APPLETON_LAT_LONG.0,
            APPLETON_LAT_LONG.1, 0.0, &coord_transform);
        assert!(result.is_ok());

        let coordinates = result.unwrap();
        assert!((coordinates.0 - APPLETON_MERCATOR.0).abs() < 0.01);
        assert!((coordinates.1 - APPLETON_MERCATOR.1).abs() < 0.01);
    }

    // TODO - transform pixel

    // TODO - transform pixels

    #[test]
    fn geohash_intervals() {
        let geocode = Geocode::Geohash;
        assert_eq!(geocode.get_intervals(1),
            (45.0, 45.0));
        assert_eq!(geocode.get_intervals(2),
            (11.25, 5.625));
        assert_eq!(geocode.get_intervals(3),
            (1.40625, 1.40625));
        assert_eq!(geocode.get_intervals(4),
            (0.3515625, 0.17578125));
        assert_eq!(geocode.get_intervals(5),
            (0.0439453125, 0.0439453125));
        assert_eq!(geocode.get_intervals(6),
            (0.010986328125, 0.0054931640625));
    }

    // TODO - test get_bounds
 
    // TODO - test get_windows

    #[test]
    fn quadtile_encode() {
        let geocode = Geocode::QuadTile;

        let result = geocode.encode(
            HAMBURG_MERCATOR.0, HAMBURG_MERCATOR.1, 4);
        assert!(result.is_ok());
        assert_eq!("1202", &result.unwrap());

        let result = geocode.encode(
            FORT_COLLINS_MERCATOR.0, FORT_COLLINS_MERCATOR.1, 6);
        assert!(result.is_ok());
        assert_eq!("023101", &result.unwrap());

        let result = geocode.encode(
            APPLETON_MERCATOR.0, APPLETON_MERCATOR.1, 8);
        assert!(result.is_ok());
        assert_eq!("03022201", &result.unwrap());
    }

    #[test]
    fn quadtile_intervals() {
        let geocode = Geocode::QuadTile;
        assert_eq!(geocode.get_intervals(1),
            (20037508.342789248, 20037508.342789248));
        assert_eq!(geocode.get_intervals(2),
            (10018754.171394624, 10018754.171394624));
        assert_eq!(geocode.get_intervals(3),
            (5009377.085697312, 5009377.085697312));
        assert_eq!(geocode.get_intervals(4),
            (2504688.542848656, 2504688.542848656));
        assert_eq!(geocode.get_intervals(5),
            (1252344.271424328, 1252344.271424328));
        assert_eq!(geocode.get_intervals(6),
            (626172.135712164, 626172.135712164));
    }
}
