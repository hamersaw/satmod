use gdal::errors::Error;
use gdal::raster::{Buffer, Dataset, Driver};
use gdal::raster::types::GdalType;
use gdal_sys::GDALDataType;

pub mod coordinate;
pub mod serialize;
pub mod transform;

pub fn coverage(dataset: &Dataset) -> Result<f64, Error> {
    let (width, height) = dataset.size();
    let mut invalid_pixels = vec![true; width * height];
    
    // iterate over rasterbands
    for i in 0..dataset.count() {
        match dataset.band_type(i+1)? {
            GDALDataType::GDT_Byte => _coverage::<u8>(dataset,
                i+1, &mut invalid_pixels, 0u8)?,
            GDALDataType::GDT_UInt16 => _coverage::<u16>(dataset,
                i+1, &mut invalid_pixels, 0u16)?,
            _ => unimplemented!(),
        }
    }

    // compute percentage of valid pixels
    let pixel_count = (width * height) as f64;
    let invalid_count = invalid_pixels.iter()
        .filter(|x| **x).count() as f64;

    Ok((pixel_count - invalid_count) / pixel_count)
}

pub fn _coverage<T: Copy + GdalType + PartialEq>(dataset: &Dataset,
        index: isize, invalid_pixels: &mut Vec<bool>,
        null_value: T) -> Result<(), Error> {
    // read rasterband data into buffer
    let buffer = dataset.read_full_raster_as::<T>(index)?;

    // iterate over pixels
    for (i, pixel) in buffer.data.iter().enumerate() {
        if *pixel != null_value {
            invalid_pixels[i] = false;
        }
    }

    Ok(())
}

/*pub fn fill(rasters: &mut Vec<Buffer<u8>>,
        fill_rasters: &Vec<Buffer<u8>>)
        -> Result<(), Box<dyn std::error::Error>> {
    // iterate over pixels
    let size = rasters[0].data.len();
    for i in 0..size {
        if fill_rasters[0].data.len() <= i {
            break;
        }

        // check if rasterband pixel is valid
        let mut valid = false;
        for j in 0..rasters.len() {
            valid = valid || rasters[j].data[i] != 0u8;
        }

        // copy pixels from fill_raster bands
        if !valid {
            for j in 0..rasters.len() {
                rasters[j].data[i] = fill_rasters[j].data[i];
            }
        }
    }

    Ok(())
}*/

fn init_dataset(driver: &Driver, filename: &str,
        gdal_type: GDALDataType::Type, width: isize, height: isize,
        rasterband_count: isize) -> Result<Dataset, Error> {
    match gdal_type {
        GDALDataType::GDT_Byte => driver.create_with_band_type::<u8>
            (filename, width, height, rasterband_count),
        GDALDataType::GDT_UInt16 => driver.create_with_band_type::<u16>
            (filename, width, height, rasterband_count),
        _ => unimplemented!(),
    }
}

fn copy_raster(src_dataset: &Dataset, src_index: isize,
        src_window: (isize, isize), src_window_size: (usize, usize),
        dst_dataset: &Dataset, dst_index: isize, 
        dst_window: (isize, isize), dst_window_size: (usize, usize))
        -> Result<(), Error> {
    match src_dataset.band_type(src_index)? {
        GDALDataType::GDT_Byte => _copy_raster::<u8>(src_dataset, 
            src_index, src_window, src_window_size, dst_dataset, 
            dst_index, dst_window, dst_window_size),
        GDALDataType::GDT_UInt16 => _copy_raster::<u16>(src_dataset, 
            src_index, src_window, src_window_size, dst_dataset, 
            dst_index, dst_window, dst_window_size),
        _ => unimplemented!(),
    }
}

fn _copy_raster<T: Copy + GdalType>(src_dataset: &Dataset,
        src_index: isize, src_window: (isize, isize), 
        src_window_size: (usize, usize), dst_dataset: &Dataset,
        dst_index: isize, dst_window: (isize, isize), 
        dst_window_size: (usize, usize)) -> Result<(), Error> {
    // read rasterband data into buffer
    let buffer = src_dataset.read_raster_as::<T>(src_index,
        src_window, src_window_size, dst_window_size)?;

    dst_dataset.write_raster::<T>(dst_index,
        dst_window, dst_window_size, &buffer)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    /*use gdal::raster::{Dataset, Driver};
    use gdal_sys::GDALDataType;

    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::Path;

    #[test]
    fn transfer() {
        //let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        //let path = Path::new("examples/T13TDF_20150821T180236_B01.jp2");
        let path = Path::new("/tmp/st-image-0.tif");

        // read dataset
        let dataset = Dataset::open(path).expect("dataset open");

        // write dataset to buffer
        let mut buffer = Vec::new();
        super::write(&dataset, &mut buffer).expect("dataset write");

        // read dataset from buffer
        let mut cursor = Cursor::new(buffer);
        let read_dataset = super::read(&mut cursor)
            .expect("dataset read");

        // open gtiff driver
        let driver = Driver::get("GTiff").expect("get driver");
        read_dataset.create_copy(&driver, "/tmp/st-image-transfer", None)
            .expect("dataset copy");
    }*/
}
