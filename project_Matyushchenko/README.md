Дивлячись на данні які отримав наш Data відділ з метою проаналізувати продажі за географічною локацією покупців (по штатах) та за віком. Так як вони надходили з 2 джерел даних: customers та sales. Було прийнято рішення побудувати 2 пайплайни даних.
process_sales pipeline
process_customers pipeline

Аналізуючи дані помічаєм, що клієнти не заповнювали деякі дані, тому щоб вирішити описані вище проблеми, ми домовляємось про інтеграцію третього джерела даних: user_profiles:
process_user_profiles pipeline
Після того, як дані всі процесяться успішно, збагатили дані customers за допомогою даних з user_profiles
Далі створили єдиний пайплайн, який пише дані в рівень gold. В датасеті gold  створили таблицю user_profiles_enriched в результаті роботи цього пайплайну.
Для переміщення даних використовували Airflow
В решті-решт, коли всі пайплайни побудовані, можемо дати відповідь на наступну аналітичну задачу:
“В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду вересня?”

Відповідь наступна: в штаті "Idaho" було куплено найбільше (179) телевізорів покупцями від 20 до 30 років за першу декаду вересня.


