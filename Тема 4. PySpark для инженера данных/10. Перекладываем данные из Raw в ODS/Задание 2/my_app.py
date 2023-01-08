import sys
from datetime import date as dt

def main():
        client_name = sys.argv[1]
        date = sys.argv[2]
        dir_name = sys.argv[3]

# Добавляем переменные в код

        if str(dt.today()) == date:
            print(f"Hi {client_name}!")
            current_dir = dir_name
            print(f'Your current directory is {current_dir}')
        else:
            print("Come back another day")

# Если дата, передаваемая через командную строку, совпадёт с актуальной датой,
# то программа поздоровается с пользователем и передаст название текущей директории.
# В противном случае попросит прийти в другой день.

        
# При запуске файла будет выполняться код ниже после двоеточия
# Конкретно здесь - функция main()

if __name__ == "__main__":
        main()
