import unittest
import json
import os
import shutil

class InvertedIndexTestClass(unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName=methodName)

        # Current path
        PATH = os.getcwd()
        
        # Relative paths to dirs where input files are stored
        self.INPUT_BASE_DIR = PATH+"/input_files/"
        self.INPUT_DIR_1 = self.INPUT_BASE_DIR+"input_files_1"
        self.INPUT_DIR_2 = self.INPUT_BASE_DIR+"input_files_2"
        self.INPUT_DIR_3 = self.INPUT_BASE_DIR+"input_files_3"
        self.INPUT_DIR_4 = self.INPUT_BASE_DIR+"input_files_4"
        self.INPUT_DIR_5 = self.INPUT_BASE_DIR+"input_files_5"
        
        # Relative paths to dirs where output files will be stored
        self.OUTPUT_BASE_DIR = PATH+"/output_files/"
        self.OUTPUT_DIR_1 = self.OUTPUT_BASE_DIR+"output_files_1"
        self.OUTPUT_DIR_2 = self.OUTPUT_BASE_DIR+"output_files_2"
        self.OUTPUT_DIR_3 = self.OUTPUT_BASE_DIR+"output_files_3"
        self.OUTPUT_DIR_4 = self.OUTPUT_BASE_DIR+"output_files_4"
        self.OUTPUT_DIR_5 = self.OUTPUT_BASE_DIR+"output_files_5"

    # Parsing output directories to a single json object
    def data_to_dictionary(self, data):
        list_of_jsons = [js for js in data.split('~')]
        list_of_jsons = list_of_jsons[0:len(list_of_jsons)-1]
        list_of_jsons = [json.loads(js) for js in list_of_jsons]

        result_json = {}
        for dic in list_of_jsons:
            for key in dic:
                result_json[key] = dic[key]
        return result_json

    # Creating the output dirs before every test
    def setUp(self):
        if os.path.exists(self.OUTPUT_BASE_DIR) == False:
            os.mkdir(self.OUTPUT_BASE_DIR)

    def test_reducer_input_files_1_a_f_txt(self):
        os.system("mpiexec --hostfile ../hostfile -n {} python3 ../main.py {} {}".format(5, self.INPUT_DIR_1, self.OUTPUT_DIR_1))
        
        with open(self.OUTPUT_DIR_1+"/reduce/a-f.txt", 'r') as f:
            output_data = f.read()

        json_data = self.data_to_dictionary(output_data)
        
        # Checking if the 'a' word exists in 'inputX.txt' files 1 time
        shouldBeTrue = True
        shouldBeTrue &= json_data['a']['input1.txt'] == 1
        shouldBeTrue &= json_data['a']['input2.txt'] == 1
        shouldBeTrue &= json_data['a']['input3.txt'] == 1
        shouldBeTrue &= json_data['a']['input4.txt'] == 1
        shouldBeTrue &= json_data['a']['input5.txt'] == 1

        self.assertTrue(shouldBeTrue)

    def test_reducer_input_files_1_g_p_txt(self):
        os.system("mpiexec --hostfile ../hostfile -n {} python3 ../main.py {} {}".format(5, self.INPUT_DIR_1, self.OUTPUT_DIR_1))
        
        with open(self.OUTPUT_DIR_1+"/reduce/g-p.txt", 'r') as f:
            output_data = f.read()

        json_data = self.data_to_dictionary(output_data)

        # Checking if the 'g' word exists in 'inputX.txt' files 1 time
        shouldBeTrue = True
        shouldBeTrue &= json_data['g']['input1.txt'] == 1
        shouldBeTrue &= json_data['g']['input2.txt'] == 1
        shouldBeTrue &= json_data['g']['input3.txt'] == 1
        shouldBeTrue &= json_data['g']['input4.txt'] == 1
        shouldBeTrue &= json_data['g']['input5.txt'] == 1

        self.assertTrue(shouldBeTrue)

    def test_reducer_input_files_2(self):
        os.system("mpiexec --hostfile ../hostfile -n {} python3 ../main.py {} {}".format(5, self.INPUT_DIR_2, self.OUTPUT_DIR_2))
        
        with open(self.OUTPUT_DIR_2+"/reduce/a-f.txt", 'r') as f:
            output_data = f.read()

        json_data = self.data_to_dictionary(output_data)

        shouldBeTrue = True
        # Checking if the 'care' word exists in 'input1.txt' file 1 time
        shouldBeTrue &= json_data['care']['input1.txt'] == 1
        # Checking if the 'care' word exists in 'input1.txt' file 1 time
        shouldBeTrue &= json_data['fi']['input1.txt'] == 1
        # Checking if the 'care' word exists in 'input2.txt' file 1 time
        shouldBeTrue &= json_data['ep']['input2.txt'] == 1

        with open(self.OUTPUT_DIR_2+"/reduce/g-p.txt", 'r') as f:
            output_data = f.read()

        json_data = self.data_to_dictionary(output_data)
        # Checking if the 'proiect' word exists in 'input2.txt' file 1 time
        shouldBeTrue &= json_data['proiect']['input2.txt'] == 1
        # Checking if the 'propozitie' word exists in 'input1.txt' file 1 time
        shouldBeTrue &= json_data['propozitie']['input1.txt'] == 1

        with open(self.OUTPUT_DIR_2+"/reduce/q-z.txt", 'r') as f:
            output_data = f.read()

        json_data = self.data_to_dictionary(output_data)
        # Checking if the 'testare' word exists in 'input1.txt' file 1 time
        shouldBeTrue &= json_data['testata']['input1.txt'] == 1
        # Checking if the 'va' word exists in 'input1.txt' file 1 time
        shouldBeTrue &= json_data['va']['input1.txt'] == 1
        # Checking if the 'text' word exists in 'input3.txt' file 1 time
        shouldBeTrue &= json_data['text']['input3.txt'] == 1
        # Checking if the 'random' word exists in 'input3.txt' file 1 time
        shouldBeTrue &= json_data['random']['input3.txt'] == 1

        self.assertTrue(shouldBeTrue)

    def test_reducer_input_files_3(self):
        os.system("mpiexec --hostfile ../hostfile -n {} python3 ../main.py {} {}".format(5, self.INPUT_DIR_3, self.OUTPUT_DIR_3))
        
        with open(self.OUTPUT_DIR_3+"/reduce/a-f.txt", 'r') as f:
            output_data = f.read()

        json_data = self.data_to_dictionary(output_data)
        shouldBeTrue = True
        # Checking if the 'aici' word exists in 'input1.txt' file 28 time
        shouldBeTrue &= json_data['aici']['input1.txt'] == 28
        # Checking if the 'aici' word exists in 'input2.txt' file 26 time
        shouldBeTrue &= json_data['aici']['input2.txt'] == 26
        # Checking if the 'aicinu' word exists in 'input2.txt' file 2 time
        shouldBeTrue &= json_data['aicinu']['input2.txt'] == 2

        self.assertTrue(shouldBeTrue)

    # Deleting the output dirs after every test
    def tearDown(self):
        try:
            shutil.rmtree(self.OUTPUT_BASE_DIR)
        except FileNotFoundError:
            pass