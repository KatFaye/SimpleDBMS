from data_output_stream import DataOutputStream
from column import Column


class InsertRow:
    # Constants for special offsets
    # The field with this offset is a primary key.
    IS_PKEY = -1

    # The field with this offset has a null value.
    IS_NULL = -2

    def __init__(self, table, values):
        """
        Constructs an InsertRow object for a row containing the specified
        values that is to be inserted in the specified table.
        :param table:
        :param values:
        """
        self._table = table
        self._values = values

        # These objects will be created by the marshall() method.
        self._key = None
        self._data = None

    def marshall(self):
        """
        Takes the collection of values for this InsertRow
        and marshalls them into a key/data pair.
        :return:
        """

        offset = 4 * (self._table.num_columns() + 1) # init offset
        col = 0
        self._data = DataOutputStream()

        for val in self._values:
            curr = self._table.get_column(col) # curr column accessing
            if curr.is_primary_key():
                code = -1
                self._key = DataOutputStream()
                self._data.write_int(code)
            elif type(val) == str:
                self._data.write_int(offset)
                offset += len(val) # inc. by str size

            elif type(val) == int:
                self._data.write_int(offset)
                offset += 4 # inc. by int size

            elif type(val) == float:
                self._data.write_int(offset)
                offset  += 8 # inc. by float size

            else: #default
                code = -2
                self._data.write_int(code)

            col += 1

        col = 0 # back to start~

        for val in self._values:
            curr = self._table.get_column(col)

            if curr.is_primary_key():
                self._key.write_int(val)

            if type(val) == str:
                byteVal = bytearray(val)
                for b in byteVal:
                    self._data.write_byte(b)
            elif type(val) == int:
                self._data.write_int(val)
            elif type(val) == float:
                self._data.write_float(val)
            else: # default
                continue

            col += 1

    def get_key(self):
        """
        Returns the key in the key/data pair for this row.
        :return: the key
        """
        return self._key

    def get_data(self):
        """
        Returns the data item in the key/data pair for this row.
        :return: the data
        """
        return self._data
