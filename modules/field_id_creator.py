#-------------------------------------------------------------------------------------------------------------
# Name:        field_id_creator
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# ---This class contains methods to generate unique IDs from the row content and a key,
# --and you can decrypt it back to the original content whenever needed using the key.
# ------------Concatenate Row Content: Combine the row content into a single string.
# ------------Encrypt the String: Use AES encryption with a key to create a unique ID.
# ------------Store the Unique ID: Save the encrypted string in the PostgreSQL table.
# --------Decrypt to Retrieve Original Content: Use the same key to decrypt the unique ID and retrieve the original row content.


from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import base64
import mmh3, os, re, geojson

import modules.geo_position as geo


class FieldIdCreation:

    @staticmethod
    def create_id_dict(folder_path):
        """
        Create list of ids with their dates, if single, date is 0000.
        Create dict where original field ids are mapped to the hashed values of the string containing all information
        from the geojsons.
        :param folder_path: The folder to the geojsons containing information to create the id dict.
        :return: hashed_id_orig_dict: the dictionary created.
        """

        hashed_id_orig_dict = {}

        field_id_list = os.listdir(folder_path)
        field_id_list.sort()

        for filename in field_id_list:
            if filename.endswith('.geojson'):
                filepath = os.path.join(folder_path, filename)

                start_date = end_date = "0000-00-00"
                year = "0000"
                field_id = crop_type = buff_distm = None

                # Extract information from the filename
                match = re.match(r"ZEPP_(\d+)_([A-Za-z-]+)_inBuf(\d+)m_(\d+)?\.geojson", filename)
                match2 = re.match(r"ZEPP_(\d+)_([A-Za-z-]+)_inBuf(\d+)m.geojson", filename)
                if match:
                    field_id = match.group(1)
                    crop_type = match.group(2)
                    buff_distm = int(match.group(3))
                    year = match.group(4)

                    startdate = f"{year}-01-01" if year else "0000-00-00"
                    enddate = f"{year}-12-31" if year else "0000-00-00"

                if match2:
                    field_id = match2.group(1)
                    crop_type = match2.group(2)
                    buff_distm = int(match2.group(3))

                if match or match2:

                    # Calculate area
                    area = geo.calculate_area(filepath)

                    with open(filepath, 'r') as file:
                        geojson_data = geojson.load(file)

                    # Assuming the GeoJSON contains a FeatureCollection with one polygon
                    polygon = geojson_data['coordinates'][0]

                    data = f'origin: "ZEPP", geom: {polygon}, startdate: {startdate}, enddate: {enddate}, crop_type: {crop_type}, buff_distm: {buff_distm}, size: {area}'
                    hashed_id = FieldIdCreation.hash_data(data, "43218765", year)

                    hashed_id_orig_dict[(field_id, year)] = hashed_id

        return hashed_id_orig_dict

    @staticmethod
    def concatenate_row(origin, wkt, startdate, enddate, croptype, size):
        return f"{origin},{wkt},{startdate},{enddate},{croptype},{size}"

    @staticmethod
    def hash_data(data, key, year):
        # Concatenate the key to the data
        combined_data = f"{data}{key}"
        # Generate a 32-bit hash
        hashed_value = mmh3.hash(combined_data)

        if hashed_value < 0:
            hashed_value += 2 ** 32

        # Append the year to the hash value
        # Convert the year to a string and get the last two digits
        year_suffix = int(str(year)[-2:])

        # Create the final hash by combining the hash value and the year suffix
        final_hash = (hashed_value * 100) + year_suffix
        return final_hash

    @staticmethod
    def encrypt_data(data, key):
        # Ensure the key is 32 bytes (256 bits) for AES-256
        key = key.ljust(32, '0')[:32].encode('utf-8')
        iv = b'0123456789abcdef'  # Initialization vector (must be 16 bytes)
        cipher = Cipher(algorithms.AES(key), modes.CFB(iv), backend=default_backend())
        encryptor = cipher.encryptor()

        # Pad the data to be AES block size compatible
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(data.encode()) + padder.finalize()

        # Encrypt the data
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

        # Encode the encrypted data with base64 to make it storable
        encrypted_data_base64 = base64.b64encode(encrypted_data).decode('utf-8')

        return encrypted_data_base64

    @staticmethod
    def decrypt_data(encrypted_data, key):
        # Ensure the key is 32 bytes (256 bits) for AES-256
        key = key.ljust(32, '0')[:32].encode('utf-8')
        iv = b'0123456789abcdef'  # Initialization vector (must be the same as used for encryption)
        cipher = Cipher(algorithms.AES(key), modes.CFB(iv), backend=default_backend())
        decryptor = cipher.decryptor()

        # Decode the base64 encoded data
        encrypted_data = base64.b64decode(encrypted_data)

        # Decrypt the data
        padded_data = decryptor.update(encrypted_data) + decryptor.finalize()

        # Unpad the data
        unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
        data = unpadder.update(padded_data) + unpadder.finalize()

        return data.decode('utf-8')

    @staticmethod
    def encrypt_decrypt_information():
        # Example usage
        data = 'origin: "ZEPP", geom: POINT(1 1), startdate: 2022-01-01, enddate: 2022-12-31, crop_type: wheat, buff_distm: 100, size: 50'
        key = '52367397'  # 16-byte key for AES-128

        encrypted_data = FieldIdCreation.encrypt_data(data, key)
        print(f'Encrypted data: {encrypted_data}')

        # Example usage
        decrypted_data = FieldIdCreation.decrypt_data(encrypted_data, key)
        print(f'Decrypted data: {decrypted_data}')

    @staticmethod
    def hash_from_geojson(geojson_file_path, crop_type):
        """
        This is the method that provides the information to how the ids derive from the geojson content and data origin
        information.
        :param geojson_file_path: The path to the geojson.
        :param crop_type: The relevant crop type handled.
        :return:
        """

        # Calculate area
        area = geo.calculate_area(geojson_file_path)

        with open(geojson_file_path, 'r', encoding='utf-8') as geojson_file:
            geojson_data = geojson.load(geojson_file)
            geometry = geojson_data.get("geometry")
            polygon = geometry['coordinates'][0]

            properties = geojson_data.get("properties", {})
            year = int(float(properties.get("Year")))

            startdate = f"{str(year)}-01-01" if year else "0000-00-00"
            enddate = f"{str(year)}-12-31" if year else "0000-00-00"

            data = f'origin: "ZEPP", geom: {polygon}, startdate: {startdate}, enddate: {enddate}, crop_type: {crop_type}, buff_distm: {0}, size: {area}'
            return FieldIdCreation.hash_data(data, "43218765", year)