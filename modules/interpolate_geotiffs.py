# --------------------------------------------------------------------------------------------------------------------------------
# Name:        interpolate_geotiffs
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------

import os
import json
import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import shape
from scipy.interpolate import griddata
from contextlib import contextmanager
from rasterio.plot import show
import matplotlib.pyplot as plt


class InterpolateGeotiffs:
    """
    A utility class for interpolating, saving and plotting geotiff files.
    """

    @staticmethod
    def grid_interpolation(arr):
        """
        Interpolates missing values in the array using cubic interpolation.

        Parameters:
            arr (numpy.ndarray): The input array.

        Returns:
            numpy.ndarray: The interpolated array.
        """

        # Create some holes for testing different interpolation methods
        arr = arr.astype(np.float32)
        arr[arr == 0] = np.nan

        x = np.arange(0, arr.shape[1])
        y = np.arange(0, arr.shape[0])

        # mask invalid values
        arr_m = np.ma.masked_invalid(arr)
        xx, yy = np.meshgrid(x, y)

        # get only the valid values
        x1 = xx[~arr_m.mask]
        y1 = yy[~arr_m.mask]
        new_arr = arr_m[~arr_m.mask]
        ravel = new_arr.ravel()

        grid = griddata((x1, y1), ravel, (xx, yy), method='cubic', fill_value=0)
        grid = np.rint(grid).astype(np.integer)
        return grid

    @staticmethod
    def calculate_valid_pixels(path_to_geojson, raster_file, nan_value=6.9055e-41):
        """
        Calculate the amount of valid pixels of a raster file within a polygon boundary.

        Parameters:
            path_to_geojson (str): Path to the GeoJSON file containing the polygon.
            raster_file (str): Path to the raster geotiff file.
            nan_value (float): Value representing invalid pixels in the raster file.

        Returns:
            tuple: A tuple containing the amount of valid pixels and the maximum possible amount of valid pixels.
        """

        # Open the raster file
        with InterpolateGeotiffs.open_raster_and_geojson(raster_file, path_to_geojson) as (src, json_file):
            polygon_geojson = json.load(json_file)
            polygon = shape(polygon_geojson)

            # Mask the raster using the polygon boundary
            masked_data, _ = mask(src, [polygon], crop=True, nodata=nan_value)

            # Calculate the amount of valid pixels
            valid_pixels = np.count_nonzero(masked_data != nan_value)

            # Calculate the total number of pixels in each band
            total_pixels_per_band = np.prod(masked_data.shape[1:])

            # Calculate the total number of pixels for all bands
            total_pixels = total_pixels_per_band * masked_data.shape[0]

        return valid_pixels, total_pixels

    @staticmethod
    @contextmanager
    def open_raster_and_geojson(raster_file, geojson_file):
        """
        Open the raster and GeoJSON files.

        Parameters:
            raster_file (str): Path to the raster file.
            geojson_file (str): Path to the GeoJSON file.

        Yields:
            tuple: A tuple containing the opened raster and GeoJSON files.
        """
        with rasterio.open(raster_file) as src:
            with open(geojson_file, "r") as json_file:
                yield src, json_file

    @staticmethod
    def valid_pixel_in_poly(path_to_geojson, path_to_geotiff, no_data_value=6.9055e-41):
        """
            Calculates the amount of raster pixel that are not no_data_value and inside the given geojson borders.

            Parameters:
                path_to_geojson (str): Path to the GoeJSON border object.
                path_to_geotiff (str): Path to the GeoTIFF file.

            Yields:
                tuple: A tuple containing the opened raster and GeoJSON files.
            """
        amount_pixel_data = 0
        amount_pixel_mask = 0

        with InterpolateGeotiffs.open_raster_and_geojson(path_to_geotiff, path_to_geojson) as (opened_data_tiff, file):

            polygon_geojson = json.load(file)
            polygon = shape(polygon_geojson)
            meta = opened_data_tiff.meta

            max_pixel = 0

            # This is only for S2 data
            ran = len(opened_data_tiff.indexes)

            for i in range(0, ran-1):
                band = opened_data_tiff.read(i+1)
                amount_pixel_data = np.count_nonzero(band != no_data_value)
                max_pixel = amount_pixel_data if amount_pixel_data > max_pixel else max_pixel

            if ran == 1:
                band = opened_data_tiff.read(1)
                amount_pixel_data = np.count_nonzero(band != no_data_value)

            with rasterio.open("mask", 'w', **meta) as dest:
                print("Mask created")

            with rasterio.open("mask", 'r+', **meta) as dest1:
                data = dest1.read()
                data[data == no_data_value] = 1

                for i in range(data.shape[0]):
                    dest1.write(data[i], i + 1)

                if polygon:
                    masked, out_trans = mask(dest1, [polygon], nodata=no_data_value, crop=True)
                    amount_pixel_mask = np.count_nonzero(masked[0] != no_data_value)

        return amount_pixel_data, amount_pixel_mask

    @staticmethod
    def interpolate_tiffs(output_folder, folder_to_tiffs, path_to_geojson, min_amount_pixel):
        """
        Interpolates geotiff files in a folder based on a GeoJSON polygon.

        Parameters:
            output_folder (str): Path to the output folder.
            folder_to_tiffs (str): Path to the folder containing input tiff files.
            path_to_geojson (str): Path to the GeoJSON file.
            min_amount_pixel (float): Minimum amount of pixels required for interpolation.
        """

        geotiff_list = os.listdir(folder_to_tiffs)
        geotiff_list.sort()

        for j in range(0, len(geotiff_list)):
            current_field_item = folder_to_tiffs + geotiff_list[j]

            if not current_field_item.endswith(".tif"):
                continue

            pixel_data, pixel_mask = InterpolateGeotiffs().calculate_valid_pixels(path_to_geojson, current_field_item, 0)

            rel = pixel_data / pixel_mask

            if min_amount_pixel < rel < 1:
                print(str(geotiff_list[j]) + " can be interpolated.")

                src = rasterio.open(current_field_item)
                meta = src.meta
                arr_orig = src.read()

                ran = len(src.indexes)

                for i in range(0, ran):
                    arr = src.read(i + 1)
                    new_arr = InterpolateGeotiffs().grid_interpolation(arr)
                    arr_orig[i] = new_arr

                output_path = os.path.join(output_folder, geotiff_list[j])
                with rasterio.open(output_path, 'w', **meta) as dest1:
                    for i in range(len(src.indexes)):
                        dest1.write(arr_orig[i], i + 1)

    @staticmethod
    def interpolate_tiff(geotiff, output_folder):
        """
            Interpolates a geotiff and saves in a folder. Name the output file with extension ":interp"

            Parameters:
                geotiff (str): Path to the geotiff
                output_folder (str): Path to the output folder.

        """

        output_path = os.path.join(output_folder, os.path.basename(geotiff).replace(".tif", "_interp.tif"))
        if os.path.exists(output_path):
            return output_path

        src = rasterio.open(geotiff)
        meta = src.meta
        arr_orig = src.read()

        ran = len(src.indexes)

        for i in range(0, ran):
            arr = src.read(i + 1)
            new_arr = InterpolateGeotiffs().grid_interpolation(arr)
            arr_orig[i] = new_arr

        with rasterio.open(output_path, 'w', **meta) as dest1:
            for i in range(len(src.indexes)):
                dest1.write(arr_orig[i], i + 1)
        return output_path

    @staticmethod
    def print_raster_info(rasterio_raster):
        """
            Print the raster metadata to output to be able to compare table input raster meta data with output raster meta data.
            Not all available metadata is neccessarily printed in notation.

            Parameters:
                rasterio_raster
        """

        num_bands = rasterio_raster.count
        print('Number of bands in image: {n}\n'.format(n=num_bands))

        rows, cols = rasterio_raster.shape
        print('Image size is: {r} rows x {c} columns\n'.format(r=rows, c=cols))

        desc = rasterio_raster.descriptions
        metadata = rasterio_raster.meta

        print('Raster description: {desc}\n'.format(desc=desc))

        driver = rasterio_raster.driver
        print('Raster driver: {d}\n'.format(d=driver))

        proj = rasterio_raster.crs
        print('Image projection:')
        print(proj, '\n')

        gt = rasterio_raster.transform

        print('Image geo-transform:\n{gt}\n'.format(gt=gt))

        print('All raster metadata:')
        print(metadata)
        print('\n')

    @staticmethod
    def plot_geotiffs(file_paths):
        # Example usage with multiple GeoTIFF files

        #    InterpolateGeotiffs.plot_geotiff(file_paths[0])

        #    InterpolateGeotiffs.plot_geotiff(file_paths[1])

        #    InterpolateGeotiffs.plot_geotiff(file_paths[2])

        InterpolateGeotiffs.plot_multiple_geotiffs(file_paths)

    @staticmethod
    def plot_multiple_geotiffs(geotiff_path_list):
        """
            Print the raster metadata to output to be able to compare table input raster metadata with output raster metadata.
            Not all available metadata is necessarily printed in notation.

            Parameters:
                geotiff_path_list (list): A list of strings/paths to geoTiffs
        """

        num_plots = len(geotiff_path_list)
        fig, axes = plt.subplots(1, num_plots, figsize=(6*num_plots, 6))

        for i, file_path in enumerate(geotiff_path_list):

            # Open the GeoTIFF file
            with rasterio.open(file_path) as src:
                file_name = os.path.basename(file_path)

                # Get the affine transformation matrix
                transform = src.transform
                amount_bands = src.count

                if amount_bands == 1:

                    # Read the image data
                    band = src.read(1)
                    # Normalize the band data to range between 0 and 1
                    band = band / band.max()

                    # Plot the RGB image on the current subplot
                    ax = axes[i] if num_plots > 1 else axes
                    show(band, transform=transform, ax=ax, cmap='viridis')

                elif amount_bands == 2:

                    # Read the first 2 bands
                    red = src.read(1)
                    blue = src.read(2)

                    # Normalize the band data to range between 0 and 1
                    band1 = InterpolateGeotiffs.normalize_band(red)
                    band2 = InterpolateGeotiffs.normalize_band(blue)

                    # Create an RGB image where:
                    # - Band 1 is used for both the Red and Green channels (to create a grayscale image)
                    # - Band 2 is used for the Blue channel (to overlay additional information)
                    rgb = np.dstack((band1, band1, band2))

                    # Plot the RGB image on the current subplot
                    ax = axes[i] if num_plots > 1 else axes

                    print("2 Band:" + str(axes[i]))
                    ax.imshow(rgb)

                elif amount_bands >= 2:

                    # Read the first 3 bands
                    red = src.read(1)
                    green = src.read(2)
                    blue = src.read(3)

                    # Normalize the band data to range between 0 and 1
                    red = red / red.max()
                    green = green / green.max()
                    blue = blue / blue.max()

                    # Stack bands to form an RGB image
                    rgb = np.dstack((red, green, blue))

                    # Plot the RGB image on the current subplot
                    ax = axes[i] if num_plots > 1 else axes
                    ax.imshow(rgb)

            # Set axis labels
            ax.set_xlabel('Easting (meters)')
            ax.set_ylabel('Northing (meters)')

            # Set the title
            ax.set_title(f'GeoTIFF RGB Image: {file_name.replace(".geotiff", "")}')

        # Adjust layout and show the plot
        plt.tight_layout()
        plt.show()

    @staticmethod
    def plot_geotiff(file_path):

        file_name = os.path.basename(file_path)

        # Open the GeoTIFF file
        with rasterio.open(file_path) as src:
            # Get the affine transformation matrix
            transform = src.transform
            amount_bands = src.count

            # Plot the image using matplotlib
            fig, ax = plt.subplots(figsize=(7, 7))

            if amount_bands == 1:
                # Read the image data
                image = src.read(1)
                show(image, transform=transform, ax=ax, cmap='viridis')

            elif amount_bands == 2:
                # Read the first 2 bands
                red = src.read(1)
                blue = src.read(2)

                # Normalize the band data to range between 0 and 1
                band1 = InterpolateGeotiffs.normalize_band(red)
                band2 = InterpolateGeotiffs.normalize_band(blue)

                # Create an RGB image where:
                # - Band 1 is used for both the Red and Green channels (to create a grayscale image)
                # - Band 2 is used for the Blue channel (to overlay additional information)
                rgb = np.dstack((band1, band1, band2))

                # Stack bands to form an RGB image
                rgb = np.dstack((band1, band1, band2))
                ax.imshow(rgb)

            elif amount_bands > 2:
                # Read the first 3 bands
                red = src.read(1)
                green = src.read(2)
                blue = src.read(3)

                # Normalize the band data to range between 0 and 1
                red = red / red.max()
                green = green / green.max()
                blue = blue / blue.max()

                # Stack bands to form an RGB image
                rgb = np.dstack((red, green, blue))
                ax.imshow(rgb)

            # Set axis labels
            ax.set_xlabel('Easting (meters)')
            ax.set_ylabel('Northing (meters)')

            # Set the title
            # Set the title
            ax.set_title(f'GeoTIFF RGB Image: {file_name.replace(".geotiff","")}')

            # Show the plot
            plt.show()

    @staticmethod
    def plot_multiple_raster(raster_list):
        num_plots = len(raster_list)
        fig, axes = plt.subplots(1, num_plots, figsize=(6 * num_plots, 6))

        for i, src in enumerate(raster_list):
            # Get the affine transformation matrix
            transform = src.transform
            amount_bands = src.count

            # Calculate the extent of the raster
            extent = [transform[2], transform[2] + transform[0] * src.width,
                     transform[5] + transform[4] * src.height, transform[5]]

            if amount_bands == 1:
                # Read the image data
                image = src.read(1)

                # Plot the grayscale image on the current subplot
                ax = axes[i] if num_plots > 1 else axes

                # Set the title
                ax.set_title('Viridis Image: Coherence VV')

                # Show the image with the correct extent
                show(image, transform=transform, ax=ax, cmap='viridis')

            elif amount_bands == 2:
                # Read the first 2 bands
                red = src.read(1)
                blue = src.read(2)

                # Normalize the band data to range between 0 and 1
                band1 = InterpolateGeotiffs.normalize_band(red)
                band2 = InterpolateGeotiffs.normalize_band(blue)

                # Create an RGB image
                rgb = np.dstack((band1, band1, band2))

                # Plot the RGB image on the current subplot
                ax = axes[i] if num_plots > 1 else axes

                # Set the title
                ax.set_title(f'RGB Image: Backscatter')
                ax.imshow(rgb, extent=extent)

            elif amount_bands > 2:
                # Read the first 3 bands
                red = src.read(3)
                green = src.read(2)
                blue = src.read(1)

                # Normalize the band data to range between 0 and 1
                red = red / red.max()
                green = green / green.max()
                blue = blue / blue.max()

                # Stack bands to form an RGB image
                rgb = np.dstack((red, green, blue))

                # Plot the RGB image on the current subplot
                ax = axes[i] if num_plots > 1 else axes

                # Set the title
                ax.set_title(f'RGB Image: Sentinel 2 Optical')
                ax.imshow(rgb, extent=extent)

            # Set axis labels
            ax.set_xlabel('Easting (meters)')
            ax.set_ylabel('Northing (meters)')

        # Adjust layout and show the plot
        plt.tight_layout()
        plt.show()

    @staticmethod
    def normalize_band(band):
        # Calculate the minimum and maximum values of the band
        min_value = np.min(band)
        max_value = np.max(band)

        # Normalize the band data to range between 0 and 1
        normalized_band = (band - min_value) / (max_value - min_value)

        return normalized_band
