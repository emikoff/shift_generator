from scheduler import DataPipeline, AssignmentEngine, SchedulerReport
import pandas as pd

target_week = 2

# 1. Загрузка датавреймов
print("1. Загрузка данных")
workers = pd.read_csv("workers.csv")
equipment = pd.read_csv("equipment.csv")
schedule = pd.read_csv("schedule_template.csv")
requirements = pd.read_csv("position_requirements.csv")
plan = pd.read_csv("plan.csv")

# 2. Подготавливаем данные
data_pipeline = DataPipeline(workers, equipment, schedule, requirements, plan)
data_pipeline.run(target_week)

# 3. Запускаем процесс генерации графика работы для недели
#    Передаем ГОТОВЫЕ данные из pipeline в engine
assignment_engine = AssignmentEngine(
    data_pipeline.shift_candidates,
    data_pipeline.shift_equipment_day,
    data_pipeline.shift_equipment_evening,
    data_pipeline.shift_equipment_night,
)

assignment_engine.run()


# 4. Формирование отчета
print("\n4. Получение результатов")
report = SchedulerReport(
    assignment_engine.shift_equipment_night,
    assignment_engine.shift_equipment_day,
    assignment_engine.shift_equipment_evening,
    data_pipeline.workers,
)
# Получаем итоговые назначения
final_assignments = report.get_final_assignments()
print("\nИтоговые назначения:")
print(final_assignments.to_string())

# Получаем сводку по бригадам
brigade_summary = report.get_brigade_summary()
print("\nСводка по бригадам:")
print(brigade_summary.to_string(index=False))

# Получаем незаполненные позиции
unfilled = report.get_unfilled_positions()
print(f"\nВсего незаполненных позиций: {len(unfilled)}")

# 5. Обновление файлов ---
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
