from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()

class SensorData(Base):
    """
    Класс для представления таблицы sensor_data в базе данных.
    """

    __tablename__ = 'sensor_data'

    id = Column(Integer, primary_key=True)
    current_value_counter = Column(Integer)
    pressure_value = Column(Float)
    status = Column(String)


class SensorDataParser:
    """
    Класс для разбора данных и сохранения их в базе данных.
    """

    def __init__(self, db_params):
        """
        Инициализация экземпляра класса.

        :param db_params: Словарь с параметрами для подключения к базе данных.
        """
        self.db_params = db_params
        self.engine = create_engine(self.get_connection_string())
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def parse_hex_and_save_data(self, data):
        """
        Принимает данные в формате HEX, разбирает их и сохраняет в базу данных.

        :param data: Данные в формате HEX.
        """
        packets = self.parse_hex_packets(data)
        self.save_to_db(packets)

    def parse_hex_packets(self, data, identifier='80'):
        """
        Разбирает пакеты данных в формате HEX.

        :param data: Данные в формате HEX.
        :param identifier: Идентификатор пакета (по умолчанию '80').
        :return: Список кортежей с информацией о пакетах.
        """
        packets, packet = [], []
        for i in range(0, len(data), 2):
            if data[i:i+2] == identifier or len(packet) == 4:
                packet = []
            packet.append(data[i:i+2])
            if len(packet) == 4 and packet[0] == identifier:
                packet_counter, pressure = self.data_transform(packet)
                packets.append((identifier, packet_counter, pressure))
        return packets

    def data_transform(self, packet):
        """
        Преобразует данные из формата HEX в числа.

        :param packet: Пакет данных в формате HEX.
        :return: Кортеж с преобразованными данными (packet_counter, pressure).
        """
        packet_counter = int(packet[1], 16)
        pressure = float(f'{packet[2]}.{packet[3]}')
        return packet_counter, pressure

    def save_to_db(self, valid_packets):
        """
        Сохраняет валидные пакеты данных в базу данных.

        :param valid_packets: Список кортежей с преобразованными данными.
        """
        try:
            for identifier, counter, pressure in valid_packets:
                data = SensorData(current_value_counter=counter, pressure_value=pressure, status=identifier)
                self.session.add(data)

            self.session.commit()
            print('Данные успешно записаны в базу.')

        except Exception as e:
            self.session.rollback()
            print(f'Ошибка: {e}')
            raise

    def get_connection_string(self):
        """
        Формирует строку подключения к базе данных.

        :return: Строка подключения.
        """
        return f"postgresql://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}/{self.db_params['database']}"


if __name__ == '__main__':
    db_params = {
        'host': 'localhost',
        'database': 'test_db',
        'user': 'test',
        'password': 'test'
    }

    # data = '34ffffff80490000804a0000804b0000824c0000804d000079f3ffff'
    data = '807b8038000080390000803a0000803b0000803c0000803d0000803e0000803f000080400000804100008042000080430000804400008045000080460000804700008048000080490000804a0000804b0000804c0000804d0000804e0000804f000080500000805100008052000080530000805400008055000080560000805700008058000080590000805a0000805b0000805c0000805d0000805e0000805f000080600000806100008062000080630000806400008065000080660000806700008068000080690000806a0000806b0000806c0000806d0000806e0000806f000080700000807100008072000080730000807400008075000080760000807700008078000080790000807a0000807b0000807c0000807d0000807e0000807f000080000000800100008002000080030000800400008005000080060000800700008008000080090000800a0000800b0000800c0000800d0000800e0000800f000080100000801100008012000080130000801400008015000080160000801700008018000080190000801a0000801b0000801c0000801d0000801e0000801f000080200000802100008022000080230000802400008025000080260000802700008028000080290000802a0000802b0000802c0000802d0000802e0000802f0000803000008031000080320000803300008034000080350000803600008037'
    
    parser = SensorDataParser(db_params)
    parser.parse_hex_and_save_data(data)
