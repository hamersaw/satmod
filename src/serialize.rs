use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use failure::ResultExt;
use gdal::raster::{Buffer, Dataset, Driver};
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
    for i in 0..transform.len() {
        transform[i] = reader.read_f64::<BigEndian>()?;
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
    let driver = Driver::get("Mem").compat()?;
    let dataset = crate::init_dataset(&driver, "unreachable", gdal_type,
        width, height, rasterband_count, no_data_value)?;

    dataset.set_geo_transform(&transform).compat()?;
    dataset.set_projection(&projection).compat()?;
 
    // read rasterbands
    for i in 0..rasterband_count {
        read_raster(&dataset, i+1, reader)?;
    }

    Ok(dataset)
}

fn read_raster<T: Read>(dataset: &Dataset, index: isize,
        reader: &mut T) -> Result<(), Box<dyn Error>> {
    // compute raster size
    let (width, height) = dataset.size();
    let size = (width * height) as usize;

    // read raster type
    let gdal_type = reader.read_u32::<BigEndian>()?;
    match gdal_type  {
        GDALDataType::GDT_Byte => {
            let mut data = vec![0u8; size];
            reader.read_exact(&mut data)?;

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.write_raster::<u8>(index, (0, 0), (width as usize,
                height as usize), &buffer).compat()?;
        },
        GDALDataType::GDT_Int16 => {
            // read rasterband
            let mut data = Vec::new();
            for _ in 0..size {
                data.push(reader.read_i16::<BigEndian>()?);
            }

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.write_raster::<i16>(index, (0, 0), (width as usize,
                height as usize), &buffer).compat()?;
        },
        GDALDataType::GDT_UInt16 => {
            // read rasterband
            let mut data = Vec::new();
            for _ in 0..size {
                data.push(reader.read_u16::<BigEndian>()?);
            }

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.write_raster::<u16>(index, (0, 0), (width as usize,
                height as usize), &buffer).compat()?;
        },
        _ => unimplemented!(),
    }

    Ok(())
}

pub fn write<T: Write>(dataset: &Dataset, writer: &mut T)
        -> Result<(), Box<dyn Error>> {
    // write image dimensions
    let (width, height) = dataset.size();
    writer.write_u32::<BigEndian>(width as u32)?;
    writer.write_u32::<BigEndian>(height as u32)?;

    // write geo transform
    let transform = dataset.geo_transform().compat()?;
    for val in transform.iter() {
        writer.write_f64::<BigEndian>(*val)?;
    }

    // write projection
    let projection = dataset.projection();
    writer.write_u32::<BigEndian>(projection.len() as u32)?;
    writer.write(projection.as_bytes())?;

    // write gdal type and no_data value
    let rasterband = dataset.rasterband(1).compat()?;
    writer.write_u32::<BigEndian>(rasterband.band_type())?;
    match rasterband.no_data_value() {
        Some(value) => {
            writer.write_u8(1)?;
            writer.write_f64::<BigEndian>(value)?
        },
        None => writer.write_u8(0)?,
    }

    // write rasterbands
    writer.write_u8(dataset.count() as u8)?;
    for i in 0..dataset.count() {
        write_raster(dataset, i+1, writer)?;
    }

    Ok(())
}

fn write_raster<T: Write>(dataset: &Dataset, index: isize,
        writer: &mut T) -> Result<(), Box<dyn Error>> {
    let gdal_type = dataset.band_type(index).compat()?;
    writer.write_u32::<BigEndian>(gdal_type)?;

    match gdal_type {
        GDALDataType::GDT_Byte => {
            let buffer = dataset
                .read_full_raster_as::<u8>(index).compat()?;
            writer.write(&buffer.data)?;
        },
        GDALDataType::GDT_Int16 => {
            let buffer = dataset
                .read_full_raster_as::<i16>(index).compat()?;
            for pixel in buffer.data {
                writer.write_i16::<BigEndian>(pixel)?;
            }
        },
        GDALDataType::GDT_UInt16 => {
            let buffer = dataset
                .read_full_raster_as::<u16>(index).compat()?;
            for pixel in buffer.data {
                writer.write_u16::<BigEndian>(pixel)?;
            }
        },
        _ => unimplemented!(),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};
    use gdal_sys::GDALDataType;

    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::Path;

    /*#[test]
    fn transfer() {
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        //let path = Path::new("examples/T13TDF_20150821T180236_B01.jp2");

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
