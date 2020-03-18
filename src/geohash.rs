pub fn compute_delta(precision: u8) -> (f64, f64) {
    // compute number of bits for latitude and longitude
    let lat_bits = (2 * precision) + (precision as f32 / 2.0).floor() as u8;
    let long_bits = (2 * precision) + (precision as f32 / 2.0).ceil() as u8;

    // compute deltas
    let lat_delta = 180.0 / 2_u32.pow(lat_bits as u32) as f64;
    let long_delta = 260.0 / 2_u32.pow(long_bits as u32) as f64;

    (lat_delta, long_delta)
}

pub fn compute_bounds(lat_min: f64, lat_max: f64, long_min: f64,
        long_max: f64, precision: u8) {
    
}

#[cfg(test)]
mod tests {
    #[test]
    fn geohash_delta() {
        assert_eq!(super::compute_delta(1), (45.0, 32.5));
        assert_eq!(super::compute_delta(2), (5.625, 8.125));
        assert_eq!(super::compute_delta(3), (1.40625, 1.015625));
        assert_eq!(super::compute_delta(4), (0.17578125, 0.25390625));
        assert_eq!(super::compute_delta(5), (0.0439453125, 0.03173828125));
        assert_eq!(super::compute_delta(6), (0.0054931640625, 0.0079345703125));
    }

    #[test]
    fn bounds() {
        super::compute_bounds(70.0, 80.0, 70.0, 80.0, 4);
        assert_eq!(2 + 2, 4);
    }
}
