use geohash::{self, Coordinate};

mod spatial;

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
        // TODO - check coordinates for validity
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
        let bounds = spatial::get_coordinate_bounds(self.lat_min,
            self.lat_max, self.long_min, self.long_max, precision);

        // iterate over bounds
        let mut st_images = Vec::new();
        for bound in bounds {
            // add new StImage
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
        // TODO - check coordinates for validity
        StImage {
            lat_min: lat_min,
            lat_max: lat_max,
            long_min: long_min,
            long_max: long_max,
            precision: precision,
        }
    }

    pub fn geohash(&self) -> String {
        let geohash = geohash::encode(
            Coordinate{x:self.long_max, y: self.lat_max},
            self.precision as usize
        );

        geohash.unwrap()
    }

    pub fn geohash_coverage(&self) -> f64 {
        let rect = geohash::decode_bbox(&self.geohash()).unwrap();
        ((self.long_max - self.long_min) * (self.lat_max - self.lat_min))
            / (rect.width() * rect.height())
    }
}

#[cfg(test)]
mod tests {
    use super::{RawImage, Splittable};

    #[test]
    fn images() {
        let image = RawImage::new(-80.0, -70.0, 70.0, 80.0);
        for st_image in image.split(3) {
            println!("{} - {}", st_image.geohash(),
                st_image.geohash_coverage());
        }
    }
}
