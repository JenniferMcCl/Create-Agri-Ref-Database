#--------------------------------------------------------------------------------------------------------------------------------
# Name:        test_file_utils
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
#--------------------------------------------------------------------------------------------------------------------------------


from modules.file_utils import FileUtils


# Test scenarios including corner cases
def test_functions():

    # Test extract_number_from_folder_name function
    assert FileUtils.extract_number_from_folder_name(None) is None
    assert FileUtils.extract_number_from_folder_name("") is None

    folder_path_1 = "/path/to/folder_123_subfolder"
    folder_path_2 = "/path/to/another_folder_456_subfolder"

    assert FileUtils.extract_number_from_folder_name(folder_path_1) == 123
    assert FileUtils.extract_number_from_folder_name(folder_path_2) == 456

    # Test find_number_in_ordered_paths function
    ordered_paths = [
        "/path/to/folder_1_abc",
        "/path/to/folder_2_def",
        "/path/to/folder_3_ghi"
    ]
    assert FileUtils.find_number_in_ordered_paths(None, 2) is None
    assert FileUtils.find_number_in_ordered_paths(ordered_paths, None) is None
    assert FileUtils.find_number_in_ordered_paths(ordered_paths, 4) is None
    assert FileUtils.find_number_in_ordered_paths([], 2) is None
    assert FileUtils.find_number_in_ordered_paths(ordered_paths, 2) == "/path/to/folder_2_def"

    # Test read_column function
    # Add valid file path to csv containing integers in the first column with maximal 11 columns
    csv_file_path = ""
    assert FileUtils.read_column(None, 1, False) is None
    assert FileUtils.read_column("", 1, False) is None
    assert FileUtils.read_column(csv_file_path, None, False) is None
    assert FileUtils.read_column(csv_file_path, 1, None) is None
    assert FileUtils.read_column("non_existing_file.csv", 1, False) is None
    assert FileUtils.read_column(csv_file_path, 12, False) == []  # Index exceeds list length

    # Add a valid test column entry to use this test
    # assert FileUtils.read_column(csv_file_path, 1, False) == [1, 2, 3]

    # Test create_list_of_lists function
    assert FileUtils.create_list_of_lists(None) is None
    assert FileUtils.create_list_of_lists([]) == []

    original_list = ["a", "b", "", "c", "", "d", "e", "", "", "f"]
    assert FileUtils.create_list_of_lists(original_list) == [['a', 'b'], ['c'], ['d', 'e'], ['f']]

    print("All tests passed successfully!")


if __name__ == "__main__":
    test_functions()