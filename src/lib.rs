mod geohash;
pub mod prelude;

pub trait Splittable {
    fn split(&self, precision: u8) -> Vec<StImage>;
}

pub struct RawImage {
    lat_min: f64,
    lat_max: f64,
    long_min: f64,
    long_max: f64,
}

impl RawImage {
    pub fn new(lat_min: f64, lat_max: f64,
            long_min: f64, long_max: f64) -> RawImage {
        RawImage {
            lat_min: lat_min,
            lat_max: lat_max,
            long_min: long_min,
            long_max: long_max,
        }
    }
}

impl Splittable for RawImage {
    fn split(&self, precision: u8) -> Vec<StImage> {
        // compute geohash coordinate bounds
        let bounds = geohash::get_coordinate_bounds(self.lat_min,
            self.lat_max, self.long_min, self.long_max, precision);

        // iterate over bounds
        let mut st_images = Vec::new();
        for bound in bounds {
            st_images.push(StImage::new(bound.0, bound.1,
                bound.2, bound.3, precision));
        }

        st_images
    }
}

pub struct StImage {
    lat_min: f64,
    lat_max: f64,
    long_min: f64,
    long_max: f64,
    precision: u8,
}

impl StImage {
    pub fn new(lat_min: f64, lat_max: f64,
            long_min: f64, long_max: f64, precision: u8) -> StImage {
        StImage {
            lat_min: lat_min,
            lat_max: lat_max,
            long_min: long_min,
            long_max: long_max,
            precision: precision,
        }
    }
}
