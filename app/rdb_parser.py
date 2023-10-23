class RDBParser:
    rdb_data: str = None

    def __init__(self, rdb_data) -> None:
        self.rdb_data = rdb_data
    
    @staticmethod
    def __byte_to_bin(_byte):
        # return bin(int.from_bytes(_byte))[2:].zfill(8)
        return format(_byte, '08b')


    def __read_len_encoded_int(self, start_byte_i):
        last_byte_i = start_byte_i
        byte_1 = self.rdb_data[start_byte_i]
        bin1 = self.__byte_to_bin(byte_1)

        r = None

        if bin1.startswith('00'):
            # The next 6 bits represent the length
            r = int(bin1, 2)
        elif bin1.startswith('01'):
            # Read one additional byte. The combined 14 bits represent the length
            byte_2 = self.rdb_data[start_byte_i+1]
            last_byte_i = start_byte_i+1
            bin2 = self.__byte_to_bin(byte_2)
            r = int(bin1[2:] + bin2, 2)
        elif bin1.startswith('10'):
            # Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            raise NotImplementedError()
        elif bin1.startswith('11'):
            # The next object is encoded in a special format.
            # The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
            raise NotImplementedError()
        else:
            raise NotImplementedError()
        
        return r, last_byte_i

    def __read_str(self, start_byte_i, last_byte_i):
        r = ''
        for i in range(start_byte_i, last_byte_i+1):
            r += self.rdb_data[i].to_bytes().decode()

        return r, last_byte_i
    
    def __get_time(self, start_i, end_i):
        bin = ''

        for i in range(start_i, end_i+1):
            bin += self.__byte_to_bin(self.rdb_data[i])

        return int(bin, 2), end_i

    def __read_key_val(self, start_byte_i):
        r = {}
        re = {}

        cursor_i = start_byte_i
        while self.rdb_data[cursor_i].to_bytes() != b'\xff':
            cur_byte = self.rdb_data[cursor_i]

            if cur_byte.to_bytes() == b'\xfd':
                exp_time, cursor_i = self.__get_time(cursor_i+1, cursor_i+1+4)
            elif cur_byte.to_bytes() == b'\xfc':
                exp_time, cursor_i = self.__get_time(cursor_i+1, cursor_i+1+8)
            else:
                exp_time = None

            if exp_time:
                value_type = self.rdb_data[cursor_i+1]
                cursor_i += 1
            else:
                value_type = cur_byte

            if value_type != 0:
                print(value_type, value_type.to_bytes())
                raise NotImplementedError

            slen, cursor_i = self.__read_len_encoded_int(cursor_i + 1)
            key, cursor_i = self.__read_str(cursor_i+1, cursor_i+slen)

            if value_type == 0:
                slen, cursor_i = self.__read_len_encoded_int(cursor_i + 1)     
                val, cursor_i = self.__read_str(cursor_i+1, cursor_i+slen)

            r[key] = val
            if exp_time:
                re[key] = exp_time

            cursor_i += 1

        return r, re


    def parse(self) -> dict:
        assert self.rdb_data
        assert self.rdb_data.startswith(b'REDIS')

        db_resize_i = self.rdb_data.find(b'\xfb')
        db_hash_table_len, last_byte_read_i = self.__read_len_encoded_int(db_resize_i+1)
        db_expiry_table_len, last_byte_read_i = self.__read_len_encoded_int(last_byte_read_i+1)

        # print(db_hash_table_len, db_expiry_table_len)

        r_dict, re_dict = self.__read_key_val(last_byte_read_i+1)
        # print('r_dict', r_dict)
        return r_dict, re_dict
