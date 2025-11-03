from scheduler import Scheduler
import pandas as pd

# 1. Загрузка датавреймов
print("--- 1. Загрузка данных ---")
workers = pd.read_csv("workers.csv")
equipment = pd.read_csv("equipment.csv")
schedule = pd.read_csv("schedule_template.csv")
requirements = pd.read_csv("position_requirements.csv")
plan = pd.read_csv("plan.csv")

print("--- 2. Создание Scheduler ---")
# 2. Создаем класс Scheduler
scheduler = Scheduler(workers, equipment, schedule, requirements, plan)

# 3. Запускаем процесс генерации графика работы для недели 2
scheduler.run(target_week=2)

print("\n--- 4. Получение результатов ---")

# Получаем итоговые назначения
final_assignments = scheduler.get_final_assignments()
print("\nИтоговые назначения:")
print(final_assignments.to_string())

# Получаем сводку по бригадам
brigade_summary = scheduler.get_brigade_summary()
print("\nСводка по бригадам:")
print(brigade_summary.to_string(index=False))

# Получаем незаполненные позиции
unfilled = scheduler.get_unfilled_positions()
print(f"\nВсего незаполненных позиций: {len(unfilled)}")

# --- 5. Обновление файлов ---
# print("\n--- 5. Сохранение результатов ---")

# final_assignments.to_csv("assignment_output_OOP.csv", index=False, encoding="utf-8-sig")
# print("Файл 'assignment_output_OOP.csv' сохранен.")

# schedule_wo_target = schedule[schedule["week"] != 2].copy()
# updated_schedule = pd.concat(
#     [
#         schedule_wo_target,
#         final_assignments[["week", "shift", "worker_id"]].drop_duplicates(),
#     ],
#     ignore_index=True,
# )
# updated_schedule.to_csv("schedule_template_OOP.csv", index=False, encoding="utf-8-sig")
# print("Файл 'schedule_template_OOP.csv' сохранен.")
