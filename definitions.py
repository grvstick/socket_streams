import msgspec

class CRC:
    def __init__(self):
        self.poly32 = int(0xEDB88320)
        self.poly8_inv = int(0xB2)
        self.crc8_table = self._generate_crc8_table()

    def _calc_crc8_value(self, seed: int):
        crc8_value = seed

        for _ in range(8):
            if crc8_value & 0x01:
                crc8_value = (crc8_value >> 1) ^ self.poly8_inv
            else:
                crc8_value = crc8_value >> 1

        return crc8_value

    def _generate_crc8_table(self) -> int:
        crc_table = []
        for n in range(256):
            crc_table.append(self._calc_crc8_value(n))
        return crc_table

    def calc_blk_crc8(self, msg: bytearray):
        crc_value = 0

        for idx in range(len(msg)):
            crc_value = crc_value ^ msg[idx]
            crc_value = self.crc8_table[crc_value]

        return crc_value

    def _check_crc8(self, msg: bytearray, crc_under_eval):
        crc_calculated = self.calc_blk_crc8(msg)
        return crc_under_eval == crc_calculated


FRAME = bytearray([28, 29])
KEEP_ALIVE = msgspec.json.encode({"msg": "ALIVE"})
