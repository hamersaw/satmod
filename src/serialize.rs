use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::{Dataset, Driver};
use gdal::raster::Buffer;
use gdal_sys::GDALDataType;

use std::error::Error;
use std::io::{Read, Write};

pub fn read<T: Read>(reader: &mut T)
        -> Result<Dataset, Box<dyn Error>> {
    // read image dimensions
    let width = reader.read_u32::<BigEndian>()? as isize;
    let height = reader.read_u32::<BigEndian>()? as isize;

    // read geo transform
    let mut transform = [0.0f64; 6];
    for value in transform.iter_mut() {
        *value = reader.read_f64::<BigEndian>()?;
    }
 
    // read projection
    let projection_len = reader.read_u32::<BigEndian>()?;
    let mut projection_buf = vec![0u8; projection_len as usize];
    reader.read_exact(&mut projection_buf)?;
    let projection = String::from_utf8(projection_buf)?;

    // read gdal type and no_data value
    let gdal_type = reader.read_u32::<BigEndian>()?;
    let no_data_value = match reader.read_u8()? {
        0 => None,
        _ => Some(reader.read_f64::<BigEndian>()?),
    };
 
    // read rasterband count
    let rasterband_count = reader.read_u8()? as isize;

    // initialize dataset
    let driver = Driver::get("Mem")?;
    let dataset = crate::init_dataset(&driver, "unreachable", gdal_type,
        width, height, rasterband_count, no_data_value)?;

    dataset.set_geo_transform(&transform)?;
    dataset.set_projection(&projection)?;
 
    // read rasterbands
    for i in 0..rasterband_count {
        read_raster(&dataset, i+1, reader)?;
    }

    Ok(dataset)
}

fn read_raster<T: Read>(dataset: &Dataset, index: isize,
        reader: &mut T) -> Result<(), Box<dyn Error>> {
    // compute raster size
    let (width, height) = dataset.raster_size();
    let size = (width * height) as usize;

    // read raster type
    let gdal_type = reader.read_u32::<BigEndian>()?;
    match gdal_type  {
        GDALDataType::GDT_Byte => {
            let mut data = vec![0u8; size];
            reader.read_exact(&mut data)?;

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.rasterband(index)?.write::<u8>((0, 0),
                (width as usize, height as usize), &buffer)?;
        },
        GDALDataType::GDT_Int16 => {
            // read rasterband
            let mut data = Vec::new();
            for _ in 0..size {
                data.push(reader.read_i16::<BigEndian>()?);
            }

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.rasterband(index)?.write::<i16>((0, 0),
                (width as usize, height as usize), &buffer)?;
        },
        GDALDataType::GDT_UInt16 => {
            // read rasterband
            let mut data = Vec::new();
            for _ in 0..size {
                data.push(reader.read_u16::<BigEndian>()?);
            }

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.rasterband(index)?.write::<u16>((0, 0),
                (width as usize, height as usize), &buffer)?;
        },
        GDALDataType::GDT_Float32 => {
            // read rasterband
            let mut data = Vec::new();
            for _ in 0..size {
                data.push(reader.read_f32::<BigEndian>()?);
            }

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.rasterband(index)?.write::<f32>((0, 0),
                (width as usize, height as usize), &buffer)?;
        },
        _ => unimplemented!(),
    }

    Ok(())
}

pub fn write<T: Write>(dataset: &Dataset, writer: &mut T)
        -> Result<(), Box<dyn Error>> {
    // write image dimensions
    let (width, height) = dataset.raster_size();
    writer.write_u32::<BigEndian>(width as u32)?;
    writer.write_u32::<BigEndian>(height as u32)?;

    // write geo transform
    let transform = dataset.geo_transform()?;
    for val in transform.iter() {
        writer.write_f64::<BigEndian>(*val)?;
    }

    // write projection
    let projection = dataset.projection();
    writer.write_u32::<BigEndian>(projection.len() as u32)?;
    writer.write_all(projection.as_bytes())?;

    // write gdal type and no_data value
    let rasterband = dataset.rasterband(1)?;
    writer.write_u32::<BigEndian>(rasterband.band_type())?;
    match rasterband.no_data_value() {
        Some(value) => {
            writer.write_u8(1)?;
            writer.write_f64::<BigEndian>(value)?
        },
        None => writer.write_u8(0)?,
    }

    // write rasterbands
    writer.write_u8(dataset.raster_count() as u8)?;
    for i in 0..dataset.raster_count() {
        write_raster(dataset, i+1, writer)?;
    }

    Ok(())
}

fn write_raster<T: Write>(dataset: &Dataset, index: isize,
        writer: &mut T) -> Result<(), Box<dyn Error>> {
    let gdal_type = dataset.rasterband(index)?.band_type();
    writer.write_u32::<BigEndian>(gdal_type)?;

    match gdal_type {
        GDALDataType::GDT_Byte => {
            let buffer = dataset.rasterband(index)?
                .read_band_as::<u8>()?;
            writer.write_all(&buffer.data)?;
        },
        GDALDataType::GDT_Int16 => {
            let buffer = dataset.rasterband(index)?
                .read_band_as::<i16>()?;
            for pixel in buffer.data {
                writer.write_i16::<BigEndian>(pixel)?;
            }
        },
        GDALDataType::GDT_UInt16 => {
            let buffer = dataset.rasterband(index)?
                .read_band_as::<u16>()?;
            for pixel in buffer.data {
                writer.write_u16::<BigEndian>(pixel)?;
            }
        },
        GDALDataType::GDT_Float32 => {
            let buffer = dataset.rasterband(index)?
                .read_band_as::<f32>()?;
            for pixel in buffer.data {
                writer.write_f32::<BigEndian>(pixel)?;
            }
        }
        _ => unimplemented!(),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use gdal::Dataset;

    use std::io::Cursor;
    use std::path::Path;

    #[test]
    fn serialize_cycle() {
        // read dataset
        let path = Path::new("fixtures/MCD43A4.h10v04.006.tif");
        let dataset = Dataset::open(path).expect("open dataset");

        // write dataset to buffer
        let mut buffer = Vec::new();
        super::write(&dataset, &mut buffer).expect("write dataset");

        // read dataset from buffer
        let mut cursor = Cursor::new(buffer);
        let dataset2 = super::read(&mut cursor).expect("read dataset");

        // compare projections
        let projection = dataset.projection();
        let projection2 = dataset.projection();
        assert_eq!(projection, projection2);

        // compare transforms
        let transform = dataset.geo_transform();
        let transform2 = dataset2.geo_transform();
        assert_eq!(transform, transform2);
 
        // iterate over rasterbands
        for i in 1..dataset.raster_count() {
            // read bands
            let band = dataset.rasterband(i).expect("read raster");
            let band2 = dataset2.rasterband(i).expect("read raster2");

            // compare band types
            let band_type = band.band_type();
            let band_type2 = band2.band_type();
            assert_eq!(band_type, band_type2);

            // compate band data
            let data = band.read_band_as::<u8>().expect("read band");
            let data2 = band2.read_band_as::<u8>().expect("read band2");
            assert_eq!(data.data, data2.data);
        }
    }
}
