fn get_coordinate_deltas(precision: usize) -> (f64, f64) {
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

            // TODO - remove code
            // calculate subimage bounds
            /*let bound_lat_min =
                (lat_index * lat_delta).max(lat_min);
            let bound_lat_max =
                ((lat_index + 1.0) * lat_delta).min(lat_max);

            let bound_long_min =
                (long_index * long_delta).max(long_min);
            let bound_long_max =
                ((long_index + 1.0) * long_delta).min(long_max);*/
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
}

#[cfg(test)]
mod tests {
    #[test]
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
        /*for bound in bounds {
            println!("{:?}", bound);
        }*/
    }
}
