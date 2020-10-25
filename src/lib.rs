use gdal::{Dataset, Driver};
use gdal::raster::{Buffer, GdalType};
use gdal_sys::GDALDataType;

use std::error::Error;

pub mod coordinate;
pub mod serialize;
pub mod transform;

pub trait FromPrimitive {
    fn from_f64(value: f64) -> Self;
}

impl FromPrimitive for u8 {
    fn from_f64(value: f64) -> Self {
        value as u8
    }
}

impl FromPrimitive for u16 {
    fn from_f64(value: f64) -> Self {
        value as u16
    }
}

impl FromPrimitive for i16 {
    fn from_f64(value: f64) -> Self {
        value as i16
    }
}

pub fn get_coverage(dataset: &Dataset) -> Result<f64, Box<dyn Error>> {
    let (width, height) = dataset.raster_size();
    let mut invalid_pixels = vec![true; width * height];
    
    // iterate over rasterbands
    for i in 0..dataset.raster_count() {
        let rasterband = dataset.rasterband(i+1)?;
        let no_data_value = rasterband.no_data_value().unwrap_or(0.0);

        match rasterband.band_type() {
            GDALDataType::GDT_Byte => _get_coverage::<u8>(dataset,
                i+1, &mut invalid_pixels, no_data_value)?,
            GDALDataType::GDT_Int16 => _get_coverage::<i16>(dataset,
                i+1, &mut invalid_pixels, no_data_value)?,
            GDALDataType::GDT_UInt16 => _get_coverage::<u16>(dataset,
                i+1, &mut invalid_pixels, no_data_value)?,
            _ => unimplemented!(),
        }
    }

    // compute percentage of valid pixels
    let pixel_count = (width * height) as f64;
    let invalid_count = invalid_pixels.iter()
        .filter(|x| **x).count() as f64;

    Ok((pixel_count - invalid_count) / pixel_count)
}

fn _get_coverage<T: Copy + FromPrimitive + GdalType + PartialEq>(
        dataset: &Dataset, index: isize, invalid_pixels: &mut Vec<bool>,
        no_data_value: f64) -> Result<(), Box<dyn Error>> {
    let no_data_value = T::from_f64(no_data_value);

    // read rasterband data into buffer
    let buffer = dataset.rasterband(index)?.read_band_as::<T>()?;

    // iterate over pixels
    for (i, pixel) in buffer.data.iter().enumerate() {
        if *pixel != no_data_value {
            invalid_pixels[i] = false;
        }
    }

    Ok(())
}

pub fn fill(datasets: &Vec<Dataset>) -> Result<Dataset, Box<dyn Error>> {
    let rasterband = datasets[0].rasterband(1)?;
    let no_data_value = rasterband.no_data_value();

    match rasterband.band_type() {
        GDALDataType::GDT_Byte => _fill::<u8>(datasets, no_data_value),
        GDALDataType::GDT_Int16 => 
            _fill::<i16>(datasets, no_data_value),
        GDALDataType::GDT_UInt16 =>
            _fill::<u16>(datasets, no_data_value),
        _ => unimplemented!(),
    }
}

fn _fill<T: Copy + FromPrimitive + GdalType + PartialEq>(
        datasets: &Vec<Dataset>, no_data_option: Option<f64>)
        -> Result<Dataset, Box<dyn Error>> {
    let no_data_value = T::from_f64(no_data_option.unwrap_or(0.0));
    let dataset = &datasets[0];

    // read first dataset rasters
    let mut rasters = Vec::new();
    for i in 0..dataset.raster_count() {
        let raster = dataset.rasterband(i+1)?.read_band_as::<T>()?;
        rasters.push(raster);
    }

    // fill with remaining datasets
    for i in 1..datasets.len() {
        let fill_dataset = &datasets[i];

        // read fill dataset rasterbands
        let mut fill_rasters = Vec::new();
        for j in 0..fill_dataset.raster_count() {
            let fill_raster = fill_dataset.rasterband(j+1)?
                .read_band_as::<T>()?;
            fill_rasters.push(fill_raster);
        }

        // iterate over pixels
        let size = rasters[0].data.len();
        for j in 0..size {
            if fill_rasters[0].data.len() <= j {
                break;
            }

            // check if rasterband pixel is valid
            let mut valid = false;
            for k in 0..rasters.len() {
                valid = valid || rasters[k].data[j] != no_data_value;
            }

            // copy pixels from fill_raster bands
            if !valid {
                for k in 0..rasters.len() {
                    rasters[k].data[j] = fill_rasters[k].data[j];
                }
            }
        }
    }

    // open memory dataset
    let (width, height) = dataset.raster_size();
    let driver = Driver::get("Mem")?;
    let mem_dataset = crate::init_dataset(&driver, "unreachable",
        T::gdal_type(), width as isize, height as isize,
        rasters.len() as isize, no_data_option)?;

    mem_dataset.set_geo_transform(
        &dataset.geo_transform()?)?;
    mem_dataset.set_projection(
        &dataset.projection())?;

    // set rasterbands
    for (i, raster) in rasters.iter().enumerate() {
        mem_dataset.rasterband((i+1) as isize)?.write::<T>((0, 0),
            (width, height), &raster)?;
    }

    Ok(mem_dataset)
}

pub fn init_dataset(driver: &Driver, filename: &str,
        gdal_type: GDALDataType::Type, width: isize, height: isize,
        rasterband_count: isize, no_data_value: Option<f64>)
        -> Result<Dataset, Box<dyn Error>> {
    match gdal_type {
        GDALDataType::GDT_Byte => _init_dataset::<u8>(driver,
            filename, width, height, rasterband_count, no_data_value),
        GDALDataType::GDT_Int16 => _init_dataset::<i16>(driver,
            filename, width, height, rasterband_count, no_data_value),
        GDALDataType::GDT_UInt16 => _init_dataset::<u16>(driver,
            filename, width, height, rasterband_count, no_data_value),
        _ => unimplemented!(),
    }
}

pub fn _init_dataset<T: Copy + FromPrimitive + GdalType>(
        driver: &Driver, filename: &str, width: isize, height: isize,
        rasterband_count: isize, no_data_value: Option<f64>)
        -> Result<Dataset, Box<dyn Error>> {
    // create dataset
    let dataset = driver.create_with_band_type::<T>
        (filename, width, height, rasterband_count)?;

    // if no_data value exists -> write to rasterband
    if let Some(no_data_value) = no_data_value {
        let (buf_width, buf_height) = (width as usize, height as usize);
        let buffer = Buffer::new((buf_width, buf_height), 
            vec!(T::from_f64(no_data_value); buf_width * buf_height));

        // iterate over rasterbands
        for i in 0..rasterband_count {
            // write no_data buffer to rasterband
            let rasterband = dataset.rasterband(i as isize + 1)?;
            rasterband.set_no_data_value(no_data_value)?;

            rasterband.write::<T>((0, 0),
                (buf_width, buf_height), &buffer)?;
        }
    }

    Ok(dataset)
}

pub fn copy_raster(src_dataset: &Dataset, src_index: isize,
        src_window: (isize, isize), src_window_size: (usize, usize),
        dst_dataset: &Dataset, dst_index: isize, 
        dst_window: (isize, isize), dst_window_size: (usize, usize))
        -> Result<(), Box<dyn Error>> {
    match src_dataset.rasterband(src_index)?.band_type() {
        GDALDataType::GDT_Byte => _copy_raster::<u8>(src_dataset, 
            src_index, src_window, src_window_size, dst_dataset, 
            dst_index, dst_window, dst_window_size),
        GDALDataType::GDT_Int16 => _copy_raster::<i16>(src_dataset, 
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
        dst_window_size: (usize, usize)) -> Result<(), Box<dyn Error>> {
    // read rasterband data into buffer
    let src_rasterband = src_dataset.rasterband(src_index)?;
    let buffer = src_rasterband.read_as::<T>(src_window,
        src_window_size, dst_window_size)?;

    // write to new rasterband
    let dst_rasterband = dst_dataset.rasterband(dst_index)?;
    dst_rasterband.write::<T>(dst_window, dst_window_size, &buffer)?;

    // maintain rasterband metadata
    if let Some(value) = src_rasterband.no_data_value() {
        dst_rasterband.set_no_data_value(value)?;
    }

    Ok(())
}
