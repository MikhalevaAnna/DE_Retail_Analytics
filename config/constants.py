import os


class Config:
    # Базовые пути - определяем без создания директорий
    ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    LOGS_DIR = os.path.join(ROOT_DIR, "logs")
    GENERATOR_LOG_DIR = os.path.join(LOGS_DIR, "data_generator")
    MONGO_LOG_DIR = os.path.join(LOGS_DIR, "load_data_to_mongo")

    # Пути к лог-файлам (только строки, файлы не создаем)
    GENERATOR_LOG = "generator.log"

    # Директории для данных
    GENERATOR_DIRECTORIES = [
        os.path.join(ROOT_DIR, "source_data", "stores"),
        os.path.join(ROOT_DIR, "source_data", "products"),
        os.path.join(ROOT_DIR, "source_data", "customers"),
        os.path.join(ROOT_DIR, "source_data", "purchases"),
    ]

    # Коллекции MongoDB
    MONGO_COLLECTIONS = {
        "stores": GENERATOR_DIRECTORIES[0],
        "products": GENERATOR_DIRECTORIES[1],
        "customers": GENERATOR_DIRECTORIES[2],
        "purchases": GENERATOR_DIRECTORIES[3],
    }

    # Настройка формата логов
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # Максимальное количество покупок для генератора данных
    MAX_TARGET_PURCHASES = 500

    # Категории с эмодзи
    CATEGORIES = [
        "🥖 Зерновые и хлебобулочные изделия",
        "🥩 Мясо, рыба, яйца и бобовые",
        "🥛 Молочные продукты",
        "🍏 Фрукты и ягоды",
        "🥦 Овощи и зелень",
    ]

    # Реальные города России для распределения магазинов
    RUSSIAN_CITIES = [
        "Москва",
        "Санкт-Петербург",
        "Новосибирск",
        "Екатеринбург",
        "Казань",
        "Нижний Новгород",
        "Челябинск",
        "Самара",
        "Омск",
        "Ростов-на-Дону",
        "Уфа",
        "Красноярск",
        "Воронеж",
        "Пермь",
        "Волгоград",
        "Краснодар",
        "Саратов",
        "Тюмень",
        "Тольятти",
        "Ижевск",
        "Барнаул",
        "Ульяновск",
        "Иркутск",
        "Хабаровск",
    ]

    # Расширенный список продуктов по категориям
    PRODUCTS_BY_CATEGORY = {
        "🥖 Зерновые и хлебобулочные изделия": [
            {
                "name": "Хлеб ржаной",
                "price": (35, 60),
                "kbju": {
                    "calories": 165,
                    "protein": 6.6,
                    "fat": 1.2,
                    "carbohydrates": 33.4,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Хлеб пшеничный",
                "price": (30, 55),
                "kbju": {
                    "calories": 260,
                    "protein": 8.1,
                    "fat": 1.2,
                    "carbohydrates": 48.8,
                },
                "expiry": (3, 4),
            },
            {
                "name": "Батон нарезной",
                "price": (40, 70),
                "kbju": {
                    "calories": 262,
                    "protein": 7.5,
                    "fat": 2.9,
                    "carbohydrates": 51.4,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Лаваш армянский",
                "price": (50, 90),
                "kbju": {
                    "calories": 236,
                    "protein": 7.0,
                    "fat": 1.0,
                    "carbohydrates": 48.0,
                },
                "expiry": (5, 7),
            },
            {
                "name": "Булочка с маком",
                "price": (25, 45),
                "kbju": {
                    "calories": 310,
                    "protein": 8.0,
                    "fat": 5.0,
                    "carbohydrates": 55.0,
                },
                "expiry": (2, 3),
            },
            {
                "name": "Крупа гречневая",
                "price": (80, 120),
                "kbju": {
                    "calories": 308,
                    "protein": 12.6,
                    "fat": 3.3,
                    "carbohydrates": 57.1,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Рис круглозерный",
                "price": (70, 110),
                "kbju": {
                    "calories": 330,
                    "protein": 6.7,
                    "fat": 0.7,
                    "carbohydrates": 78.9,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Макароны",
                "price": (60, 100),
                "kbju": {
                    "calories": 340,
                    "protein": 11.0,
                    "fat": 1.3,
                    "carbohydrates": 70.0,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Овсяные хлопья",
                "price": (55, 90),
                "kbju": {
                    "calories": 366,
                    "protein": 11.9,
                    "fat": 6.1,
                    "carbohydrates": 62.0,
                },
                "expiry": (180, 365),
            },
            {
                "name": "Пшено",
                "price": (45, 75),
                "kbju": {
                    "calories": 342,
                    "protein": 11.5,
                    "fat": 3.3,
                    "carbohydrates": 66.5,
                },
                "expiry": (270, 365),
            },
            {
                "name": "Манка",
                "price": (40, 65),
                "kbju": {
                    "calories": 328,
                    "protein": 10.3,
                    "fat": 1.0,
                    "carbohydrates": 70.6,
                },
                "expiry": (300, 400),
            },
            {
                "name": "Перловка",
                "price": (35, 60),
                "kbju": {
                    "calories": 315,
                    "protein": 9.3,
                    "fat": 1.1,
                    "carbohydrates": 66.9,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Ячневая крупа",
                "price": (35, 60),
                "kbju": {
                    "calories": 313,
                    "protein": 10.4,
                    "fat": 1.1,
                    "carbohydrates": 65.4,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Кукурузные хлопья",
                "price": (90, 150),
                "kbju": {
                    "calories": 363,
                    "protein": 7.7,
                    "fat": 1.2,
                    "carbohydrates": 84.0,
                },
                "expiry": (180, 270),
            },
            {
                "name": "Сухари панировочные",
                "price": (40, 70),
                "kbju": {
                    "calories": 350,
                    "protein": 11.2,
                    "fat": 1.4,
                    "carbohydrates": 72.4,
                },
                "expiry": (180, 240),
            },
            {
                "name": "Сушки",
                "price": (80, 130),
                "kbju": {
                    "calories": 341,
                    "protein": 11.0,
                    "fat": 1.3,
                    "carbohydrates": 70.0,
                },
                "expiry": (180, 240),
            },
            {
                "name": "Пряники",
                "price": (120, 180),
                "kbju": {
                    "calories": 350,
                    "protein": 5.0,
                    "fat": 3.0,
                    "carbohydrates": 76.0,
                },
                "expiry": (60, 90),
            },
            {
                "name": "Печенье овсяное",
                "price": (100, 160),
                "kbju": {
                    "calories": 420,
                    "protein": 6.5,
                    "fat": 15.0,
                    "carbohydrates": 65.0,
                },
                "expiry": (60, 90),
            },
            {
                "name": "Вафли",
                "price": (90, 150),
                "kbju": {
                    "calories": 530,
                    "protein": 6.0,
                    "fat": 30.0,
                    "carbohydrates": 58.0,
                },
                "expiry": (90, 120),
            },
            {
                "name": "Кекс",
                "price": (70, 120),
                "kbju": {
                    "calories": 340,
                    "protein": 6.0,
                    "fat": 15.0,
                    "carbohydrates": 45.0,
                },
                "expiry": (15, 25),
            },
            {
                "name": "Ржаные хлебцы",
                "price": (60, 100),
                "kbju": {
                    "calories": 300,
                    "protein": 8.0,
                    "fat": 2.0,
                    "carbohydrates": 60.0,
                },
                "expiry": (180, 240),
            },
            {
                "name": "Кускус",
                "price": (100, 160),
                "kbju": {
                    "calories": 376,
                    "protein": 12.8,
                    "fat": 0.6,
                    "carbohydrates": 77.4,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Булгур",
                "price": (90, 150),
                "kbju": {
                    "calories": 342,
                    "protein": 12.3,
                    "fat": 1.3,
                    "carbohydrates": 75.9,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Мюсли",
                "price": (150, 250),
                "kbju": {
                    "calories": 380,
                    "protein": 10.0,
                    "fat": 8.0,
                    "carbohydrates": 65.0,
                },
                "expiry": (180, 270),
            },
        ],
        "🥩 Мясо, рыба, яйца и бобовые": [
            {
                "name": "Куриное филе",
                "price": (250, 350),
                "kbju": {
                    "calories": 110,
                    "protein": 23.0,
                    "fat": 1.2,
                    "carbohydrates": 0.0,
                },
                "expiry": (5, 7),
            },
            {
                "name": "Говядина",
                "price": (400, 600),
                "kbju": {
                    "calories": 187,
                    "protein": 18.9,
                    "fat": 12.4,
                    "carbohydrates": 0.0,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Свинина",
                "price": (300, 500),
                "kbju": {
                    "calories": 259,
                    "protein": 16.0,
                    "fat": 21.7,
                    "carbohydrates": 0.0,
                },
                "expiry": (4, 7),
            },
            {
                "name": "Фарш говяжий",
                "price": (350, 450),
                "kbju": {
                    "calories": 190,
                    "protein": 17.0,
                    "fat": 14.0,
                    "carbohydrates": 0.0,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Котлеты",
                "price": (280, 380),
                "kbju": {
                    "calories": 240,
                    "protein": 14.0,
                    "fat": 20.0,
                    "carbohydrates": 3.0,
                },
                "expiry": (5, 7),
            },
            {
                "name": "Яйцо куриное С1",
                "price": (80, 120),
                "kbju": {
                    "calories": 155,
                    "protein": 12.7,
                    "fat": 10.9,
                    "carbohydrates": 0.7,
                },
                "expiry": (25, 30),
            },
            {
                "name": "Филе индейки",
                "price": (300, 450),
                "kbju": {
                    "calories": 112,
                    "protein": 22.6,
                    "fat": 1.8,
                    "carbohydrates": 0.0,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Семга слабосоленая",
                "price": (800, 1200),
                "kbju": {
                    "calories": 203,
                    "protein": 22.5,
                    "fat": 12.5,
                    "carbohydrates": 0.0,
                },
                "expiry": (15, 25),
            },
            {
                "name": "Минтай",
                "price": (180, 250),
                "kbju": {
                    "calories": 72,
                    "protein": 15.9,
                    "fat": 0.9,
                    "carbohydrates": 0.0,
                },
                "expiry": (5, 7),
            },
            {
                "name": "Форель",
                "price": (600, 900),
                "kbju": {
                    "calories": 150,
                    "protein": 20.0,
                    "fat": 7.0,
                    "carbohydrates": 0.0,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Креветки",
                "price": (500, 800),
                "kbju": {
                    "calories": 95,
                    "protein": 19.0,
                    "fat": 1.5,
                    "carbohydrates": 0.5,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Кальмары",
                "price": (350, 550),
                "kbju": {
                    "calories": 92,
                    "protein": 18.0,
                    "fat": 1.2,
                    "carbohydrates": 2.0,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Фасоль красная",
                "price": (70, 110),
                "kbju": {
                    "calories": 337,
                    "protein": 22.5,
                    "fat": 1.5,
                    "carbohydrates": 61.0,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Горох",
                "price": (50, 80),
                "kbju": {
                    "calories": 298,
                    "protein": 20.5,
                    "fat": 2.0,
                    "carbohydrates": 53.3,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Чечевица",
                "price": (80, 130),
                "kbju": {
                    "calories": 295,
                    "protein": 24.0,
                    "fat": 1.5,
                    "carbohydrates": 46.3,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Нут",
                "price": (90, 140),
                "kbju": {
                    "calories": 364,
                    "protein": 19.0,
                    "fat": 6.0,
                    "carbohydrates": 61.0,
                },
                "expiry": (365, 730),
            },
            {
                "name": "Сосиски",
                "price": (250, 350),
                "kbju": {
                    "calories": 300,
                    "protein": 11.0,
                    "fat": 28.0,
                    "carbohydrates": 1.5,
                },
                "expiry": (15, 20),
            },
            {
                "name": "Ветчина",
                "price": (300, 450),
                "kbju": {
                    "calories": 270,
                    "protein": 15.0,
                    "fat": 22.0,
                    "carbohydrates": 1.0,
                },
                "expiry": (20, 25),
            },
            {
                "name": "Колбаса вареная",
                "price": (350, 500),
                "kbju": {
                    "calories": 250,
                    "protein": 12.0,
                    "fat": 22.0,
                    "carbohydrates": 1.5,
                },
                "expiry": (20, 30),
            },
            {
                "name": "Пельмени",
                "price": (200, 300),
                "kbju": {
                    "calories": 280,
                    "protein": 12.0,
                    "fat": 14.0,
                    "carbohydrates": 25.0,
                },
                "expiry": (30, 45),
            },
            {
                "name": "Баранина",
                "price": (450, 650),
                "kbju": {
                    "calories": 209,
                    "protein": 15.6,
                    "fat": 16.3,
                    "carbohydrates": 0.0,
                },
                "expiry": (4, 6),
            },
            {
                "name": "Скумбрия",
                "price": (250, 350),
                "kbju": {
                    "calories": 191,
                    "protein": 18.0,
                    "fat": 13.2,
                    "carbohydrates": 0.0,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Сельдь",
                "price": (180, 250),
                "kbju": {
                    "calories": 161,
                    "protein": 17.0,
                    "fat": 10.0,
                    "carbohydrates": 0.0,
                },
                "expiry": (10, 15),
            },
        ],
        "🥛 Молочные продукты": [
            {
                "name": "Молоко 3.2%",
                "price": (70, 100),
                "kbju": {
                    "calories": 60,
                    "protein": 2.9,
                    "fat": 3.2,
                    "carbohydrates": 4.7,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Кефир 2.5%",
                "price": (65, 95),
                "kbju": {
                    "calories": 50,
                    "protein": 3.0,
                    "fat": 2.5,
                    "carbohydrates": 4.0,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Творог 5%",
                "price": (90, 140),
                "kbju": {
                    "calories": 121,
                    "protein": 16.5,
                    "fat": 5.0,
                    "carbohydrates": 3.3,
                },
                "expiry": (7, 12),
            },
            {
                "name": "Сметана 20%",
                "price": (80, 130),
                "kbju": {
                    "calories": 206,
                    "protein": 2.5,
                    "fat": 20.0,
                    "carbohydrates": 3.4,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Масло сливочное",
                "price": (150, 220),
                "kbju": {
                    "calories": 748,
                    "protein": 0.5,
                    "fat": 82.5,
                    "carbohydrates": 0.8,
                },
                "expiry": (35, 45),
            },
            {
                "name": "Сыр Российский",
                "price": (500, 700),
                "kbju": {
                    "calories": 360,
                    "protein": 24.0,
                    "fat": 29.0,
                    "carbohydrates": 0.5,
                },
                "expiry": (60, 90),
            },
            {
                "name": "Йогурт питьевой",
                "price": (40, 70),
                "kbju": {
                    "calories": 80,
                    "protein": 3.0,
                    "fat": 2.5,
                    "carbohydrates": 12.0,
                },
                "expiry": (14, 20),
            },
            {
                "name": "Ряженка",
                "price": (60, 90),
                "kbju": {
                    "calories": 54,
                    "protein": 2.8,
                    "fat": 2.5,
                    "carbohydrates": 4.2,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Сливки 10%",
                "price": (70, 110),
                "kbju": {
                    "calories": 118,
                    "protein": 3.0,
                    "fat": 10.0,
                    "carbohydrates": 4.0,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Творожная масса",
                "price": (80, 130),
                "kbju": {
                    "calories": 232,
                    "protein": 7.5,
                    "fat": 12.0,
                    "carbohydrates": 25.0,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Сыр плавленый",
                "price": (60, 100),
                "kbju": {
                    "calories": 240,
                    "protein": 12.0,
                    "fat": 20.0,
                    "carbohydrates": 2.0,
                },
                "expiry": (90, 120),
            },
            {
                "name": "Мороженое пломбир",
                "price": (60, 100),
                "kbju": {
                    "calories": 232,
                    "protein": 3.7,
                    "fat": 15.0,
                    "carbohydrates": 20.4,
                },
                "expiry": (180, 240),
            },
            {
                "name": "Сгущенное молоко",
                "price": (90, 140),
                "kbju": {
                    "calories": 328,
                    "protein": 7.2,
                    "fat": 8.5,
                    "carbohydrates": 55.5,
                },
                "expiry": (365, 540),
            },
            {
                "name": "Сыр Моцарелла",
                "price": (350, 500),
                "kbju": {
                    "calories": 280,
                    "protein": 22.0,
                    "fat": 20.0,
                    "carbohydrates": 2.0,
                },
                "expiry": (30, 45),
            },
            {
                "name": "Сыр Пармезан",
                "price": (800, 1200),
                "kbju": {
                    "calories": 392,
                    "protein": 35.0,
                    "fat": 26.0,
                    "carbohydrates": 0.1,
                },
                "expiry": (180, 240),
            },
            {
                "name": "Творог зерненый",
                "price": (100, 150),
                "kbju": {
                    "calories": 110,
                    "protein": 12.0,
                    "fat": 5.0,
                    "carbohydrates": 3.0,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Кумыс",
                "price": (90, 140),
                "kbju": {
                    "calories": 50,
                    "protein": 2.5,
                    "fat": 2.0,
                    "carbohydrates": 5.0,
                },
                "expiry": (5, 7),
            },
            {
                "name": "Айран",
                "price": (70, 110),
                "kbju": {
                    "calories": 35,
                    "protein": 1.8,
                    "fat": 1.5,
                    "carbohydrates": 3.5,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Сыр Адыгейский",
                "price": (250, 350),
                "kbju": {
                    "calories": 220,
                    "protein": 18.0,
                    "fat": 16.0,
                    "carbohydrates": 1.5,
                },
                "expiry": (30, 40),
            },
            {
                "name": "Йогурт густой",
                "price": (50, 80),
                "kbju": {
                    "calories": 100,
                    "protein": 4.0,
                    "fat": 3.5,
                    "carbohydrates": 12.0,
                },
                "expiry": (14, 20),
            },
            {
                "name": "Биойогурт",
                "price": (55, 85),
                "kbju": {
                    "calories": 90,
                    "protein": 3.5,
                    "fat": 3.0,
                    "carbohydrates": 11.0,
                },
                "expiry": (14, 20),
            },
            {
                "name": "Сыр козий",
                "price": (600, 900),
                "kbju": {
                    "calories": 290,
                    "protein": 18.0,
                    "fat": 23.0,
                    "carbohydrates": 0.5,
                },
                "expiry": (45, 60),
            },
        ],
        "🍏 Фрукты и ягоды": [
            {
                "name": "Яблоки Голден",
                "price": (100, 150),
                "kbju": {
                    "calories": 52,
                    "protein": 0.3,
                    "fat": 0.2,
                    "carbohydrates": 14.0,
                },
                "expiry": (15, 30),
            },
            {
                "name": "Бананы",
                "price": (90, 130),
                "kbju": {
                    "calories": 96,
                    "protein": 1.5,
                    "fat": 0.5,
                    "carbohydrates": 21.0,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Апельсины",
                "price": (120, 180),
                "kbju": {
                    "calories": 43,
                    "protein": 0.9,
                    "fat": 0.2,
                    "carbohydrates": 9.0,
                },
                "expiry": (15, 25),
            },
            {
                "name": "Лимоны",
                "price": (80, 120),
                "kbju": {
                    "calories": 34,
                    "protein": 0.8,
                    "fat": 0.2,
                    "carbohydrates": 5.4,
                },
                "expiry": (20, 30),
            },
            {
                "name": "Груши",
                "price": (120, 180),
                "kbju": {
                    "calories": 57,
                    "protein": 0.4,
                    "fat": 0.3,
                    "carbohydrates": 15.0,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Виноград",
                "price": (150, 220),
                "kbju": {
                    "calories": 69,
                    "protein": 0.6,
                    "fat": 0.3,
                    "carbohydrates": 16.0,
                },
                "expiry": (5, 10),
            },
            {
                "name": "Мандарины",
                "price": (130, 190),
                "kbju": {
                    "calories": 38,
                    "protein": 0.8,
                    "fat": 0.3,
                    "carbohydrates": 8.1,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Клубника",
                "price": (250, 350),
                "kbju": {
                    "calories": 32,
                    "protein": 0.8,
                    "fat": 0.4,
                    "carbohydrates": 7.5,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Малина",
                "price": (300, 400),
                "kbju": {
                    "calories": 42,
                    "protein": 0.8,
                    "fat": 0.5,
                    "carbohydrates": 8.6,
                },
                "expiry": (2, 4),
            },
            {
                "name": "Киви",
                "price": (100, 150),
                "kbju": {
                    "calories": 47,
                    "protein": 0.8,
                    "fat": 0.4,
                    "carbohydrates": 8.1,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Персики",
                "price": (140, 200),
                "kbju": {
                    "calories": 45,
                    "protein": 0.9,
                    "fat": 0.1,
                    "carbohydrates": 9.5,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Нектарины",
                "price": (150, 220),
                "kbju": {
                    "calories": 44,
                    "protein": 1.1,
                    "fat": 0.3,
                    "carbohydrates": 10.6,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Гранат",
                "price": (180, 250),
                "kbju": {
                    "calories": 72,
                    "protein": 0.9,
                    "fat": 0.3,
                    "carbohydrates": 14.5,
                },
                "expiry": (20, 30),
            },
            {
                "name": "Хурма",
                "price": (130, 190),
                "kbju": {
                    "calories": 67,
                    "protein": 0.5,
                    "fat": 0.4,
                    "carbohydrates": 16.0,
                },
                "expiry": (7, 12),
            },
            {
                "name": "Ананас",
                "price": (200, 300),
                "kbju": {
                    "calories": 52,
                    "protein": 0.4,
                    "fat": 0.1,
                    "carbohydrates": 11.0,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Манго",
                "price": (220, 320),
                "kbju": {
                    "calories": 60,
                    "protein": 0.8,
                    "fat": 0.4,
                    "carbohydrates": 15.0,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Авокадо",
                "price": (180, 250),
                "kbju": {
                    "calories": 160,
                    "protein": 2.0,
                    "fat": 14.7,
                    "carbohydrates": 8.5,
                },
                "expiry": (5, 7),
            },
            {
                "name": "Черника",
                "price": (350, 450),
                "kbju": {
                    "calories": 44,
                    "protein": 0.9,
                    "fat": 0.4,
                    "carbohydrates": 7.6,
                },
                "expiry": (4, 6),
            },
            {
                "name": "Смородина черная",
                "price": (200, 280),
                "kbju": {
                    "calories": 44,
                    "protein": 1.0,
                    "fat": 0.4,
                    "carbohydrates": 7.3,
                },
                "expiry": (4, 6),
            },
            {
                "name": "Клюква",
                "price": (250, 350),
                "kbju": {
                    "calories": 28,
                    "protein": 0.5,
                    "fat": 0.2,
                    "carbohydrates": 6.0,
                },
                "expiry": (20, 30),
            },
            {
                "name": "Арбуз",
                "price": (40, 70),
                "kbju": {
                    "calories": 30,
                    "protein": 0.6,
                    "fat": 0.2,
                    "carbohydrates": 7.6,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Дыня",
                "price": (50, 90),
                "kbju": {
                    "calories": 35,
                    "protein": 0.6,
                    "fat": 0.2,
                    "carbohydrates": 8.0,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Лимон",
                "price": (80, 120),
                "kbju": {
                    "calories": 29,
                    "protein": 1.1,
                    "fat": 0.3,
                    "carbohydrates": 9.3,
                },
                "expiry": (20, 30),
            },
            {
                "name": "Лайм",
                "price": (100, 150),
                "kbju": {
                    "calories": 30,
                    "protein": 0.7,
                    "fat": 0.2,
                    "carbohydrates": 10.5,
                },
                "expiry": (15, 20),
            },
        ],
        "🥦 Овощи и зелень": [
            {
                "name": "Картофель",
                "price": (40, 70),
                "kbju": {
                    "calories": 77,
                    "protein": 2.0,
                    "fat": 0.1,
                    "carbohydrates": 17.0,
                },
                "expiry": (30, 60),
            },
            {
                "name": "Морковь",
                "price": (35, 60),
                "kbju": {
                    "calories": 35,
                    "protein": 1.3,
                    "fat": 0.1,
                    "carbohydrates": 6.9,
                },
                "expiry": (20, 40),
            },
            {
                "name": "Лук репчатый",
                "price": (30, 50),
                "kbju": {
                    "calories": 40,
                    "protein": 1.4,
                    "fat": 0.2,
                    "carbohydrates": 8.2,
                },
                "expiry": (30, 60),
            },
            {
                "name": "Капуста белокочанная",
                "price": (35, 55),
                "kbju": {
                    "calories": 28,
                    "protein": 1.8,
                    "fat": 0.1,
                    "carbohydrates": 4.7,
                },
                "expiry": (30, 45),
            },
            {
                "name": "Помидоры",
                "price": (120, 180),
                "kbju": {
                    "calories": 20,
                    "protein": 1.0,
                    "fat": 0.2,
                    "carbohydrates": 4.0,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Огурцы",
                "price": (80, 130),
                "kbju": {
                    "calories": 15,
                    "protein": 0.8,
                    "fat": 0.1,
                    "carbohydrates": 3.0,
                },
                "expiry": (7, 10),
            },
            {
                "name": "Перец болгарский",
                "price": (150, 220),
                "kbju": {
                    "calories": 27,
                    "protein": 1.3,
                    "fat": 0.2,
                    "carbohydrates": 5.3,
                },
                "expiry": (7, 12),
            },
            {
                "name": "Брокколи",
                "price": (150, 220),
                "kbju": {
                    "calories": 34,
                    "protein": 2.8,
                    "fat": 0.4,
                    "carbohydrates": 6.6,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Цветная капуста",
                "price": (140, 200),
                "kbju": {
                    "calories": 30,
                    "protein": 2.5,
                    "fat": 0.3,
                    "carbohydrates": 5.4,
                },
                "expiry": (5, 8),
            },
            {
                "name": "Шпинат",
                "price": (100, 160),
                "kbju": {
                    "calories": 23,
                    "protein": 2.9,
                    "fat": 0.4,
                    "carbohydrates": 3.6,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Чеснок",
                "price": (150, 220),
                "kbju": {
                    "calories": 149,
                    "protein": 6.4,
                    "fat": 0.5,
                    "carbohydrates": 33.0,
                },
                "expiry": (60, 90),
            },
            {
                "name": "Свекла",
                "price": (40, 65),
                "kbju": {
                    "calories": 43,
                    "protein": 1.6,
                    "fat": 0.2,
                    "carbohydrates": 8.8,
                },
                "expiry": (30, 50),
            },
            {
                "name": "Кабачки",
                "price": (60, 100),
                "kbju": {
                    "calories": 24,
                    "protein": 0.6,
                    "fat": 0.3,
                    "carbohydrates": 4.6,
                },
                "expiry": (7, 12),
            },
            {
                "name": "Баклажаны",
                "price": (80, 130),
                "kbju": {
                    "calories": 24,
                    "protein": 1.0,
                    "fat": 0.2,
                    "carbohydrates": 5.5,
                },
                "expiry": (5, 10),
            },
            {
                "name": "Тыква",
                "price": (50, 80),
                "kbju": {
                    "calories": 26,
                    "protein": 1.0,
                    "fat": 0.1,
                    "carbohydrates": 6.5,
                },
                "expiry": (30, 60),
            },
            {
                "name": "Редис",
                "price": (60, 100),
                "kbju": {
                    "calories": 16,
                    "protein": 0.7,
                    "fat": 0.1,
                    "carbohydrates": 3.4,
                },
                "expiry": (5, 7),
            },
            {
                "name": "Укроп",
                "price": (50, 80),
                "kbju": {
                    "calories": 43,
                    "protein": 3.5,
                    "fat": 1.1,
                    "carbohydrates": 7.0,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Петрушка",
                "price": (50, 80),
                "kbju": {
                    "calories": 47,
                    "protein": 3.7,
                    "fat": 0.8,
                    "carbohydrates": 7.6,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Зеленый лук",
                "price": (60, 100),
                "kbju": {
                    "calories": 32,
                    "protein": 1.3,
                    "fat": 0.1,
                    "carbohydrates": 5.7,
                },
                "expiry": (2, 4),
            },
            {
                "name": "Руккола",
                "price": (150, 220),
                "kbju": {
                    "calories": 25,
                    "protein": 2.6,
                    "fat": 0.7,
                    "carbohydrates": 2.0,
                },
                "expiry": (3, 5),
            },
            {
                "name": "Сельдерей",
                "price": (100, 150),
                "kbju": {
                    "calories": 16,
                    "protein": 0.7,
                    "fat": 0.2,
                    "carbohydrates": 3.0,
                },
                "expiry": (10, 15),
            },
            {
                "name": "Имбирь",
                "price": (150, 220),
                "kbju": {
                    "calories": 80,
                    "protein": 1.8,
                    "fat": 0.8,
                    "carbohydrates": 17.8,
                },
                "expiry": (20, 30),
            },
            {
                "name": "Хрен",
                "price": (120, 180),
                "kbju": {
                    "calories": 48,
                    "protein": 1.4,
                    "fat": 0.4,
                    "carbohydrates": 10.5,
                },
                "expiry": (30, 45),
            },
        ],
    }

    # Производители по категориям
    MANUFACTURERS_BY_CATEGORY = {
        "🥖 Зерновые и хлебобулочные изделия": [
            {"name": "АО Хлебный Дом", "inn": "7712345678", "website": "hlebnydom.ru"},
            {"name": "ООО Каравай", "inn": "7723456789", "website": "karavay.ru"},
            {
                "name": "ЗАО Русский Хлеб",
                "inn": "7734567890",
                "website": "russianbread.ru",
            },
            {"name": "ОАО Колос", "inn": "7745678901", "website": "kolos.ru"},
            {"name": "ООО Зернышко", "inn": "7756789012", "website": "zernyshko.ru"},
        ],
        "🥩 Мясо, рыба, яйца и бобовые": [
            {"name": "АО Мясокомбинат", "inn": "7712345679", "website": "myaso.ru"},
            {"name": "ООО Птицефабрика", "inn": "7723456790", "website": "ptica.ru"},
            {"name": "ЗАО Рыбный мир", "inn": "7734567891", "website": "riba.ru"},
            {"name": "ООО Мираторг", "inn": "7745678902", "website": "miratorg.ru"},
            {"name": "АПК Черкизово", "inn": "7756789013", "website": "cherkizovo.ru"},
        ],
        "🥛 Молочные продукты": [
            {
                "name": "АО Молочный Комбинат",
                "inn": "7712345680",
                "website": "moloko.ru",
            },
            {"name": "ООО Вимм-Билль-Данн", "inn": "7723456791", "website": "wbd.ru"},
            {
                "name": "ЗАО Простоквашино",
                "inn": "7734567892",
                "website": "prostokvashino.ru",
            },
            {"name": "ООО Домик в деревне", "inn": "7745678903", "website": "domik.ru"},
            {"name": "ОАО Маслозавод", "inn": "7756789014", "website": "maslo.ru"},
        ],
        "🍏 Фрукты и ягоды": [
            {
                "name": "ООО Фруктовый рай",
                "inn": "7712345681",
                "website": "fruitparadise.ru",
            },
            {
                "name": "ЗАО Сады Кубани",
                "inn": "7723456792",
                "website": "sadykubani.ru",
            },
            {"name": "АО Южные плоды", "inn": "7734567893", "website": "yugfruit.ru"},
            {"name": "ООО Мир ягод", "inn": "7745678904", "website": "yagoda.ru"},
            {
                "name": "ИП Фрукт-Импорт",
                "inn": "7756789015",
                "website": "fruitimport.ru",
            },
        ],
        "🥦 Овощи и зелень": [
            {"name": "АО Тепличный", "inn": "7712345682", "website": "teplica.ru"},
            {"name": "ООО Овощебаза", "inn": "7723456793", "website": "ovoshi.ru"},
            {
                "name": "ЗАО Зеленая грядка",
                "inn": "7734567894",
                "website": "greenbed.ru",
            },
            {"name": "ООО Витамин", "inn": "7745678905", "website": "vitamin.ru"},
            {"name": "АПК Агрокультура", "inn": "7756789016", "website": "agro.ru"},
        ],
    }

    # === 1. Генерация магазинов - 45 штук ===
    STORE_NETWORKS = [
        (
            "Большая Пикча",
            30,
            "Супермаркет площадью более 200 кв.м. Входит в федеральную сеть "
            "из 30 магазинов, некоторые из которых находятся в одном городе.",
        ),
        (
            "Маленькая Пикча",
            15,
            "Магазин у дома площадью менее 100 кв.м. Входит в федеральную сеть "
            "из 15 магазинов, некоторые из которых находятся в одном городе.",
        ),
    ]
