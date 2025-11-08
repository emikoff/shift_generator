# pyuic5 ui_main_window.ui -o ui_main_window.py
import sys
import os
import pandas as pd
from datetime import date, datetime, timedelta

# -----------------------------------------------------------------
# 1. ИМПОРТЫ QT (Используем PyQt5)
# -----------------------------------------------------------------
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox, QTableView
from PyQt5.QtCore import QAbstractTableModel, Qt, QDate, QStringListModel

# Импорт для темной темы
from PyQt5.QtGui import QPalette, QColor

# from PyQt5 import QtWidgets, QtCore
# -----------------------------------------------------------------
# 2. ИМПОРТЫ ТВОЕЙ ЛОГИКИ
# -----------------------------------------------------------------
# Импортируем твои ООП-классы из scheduler.py
from scheduler import DataPipeline, AssignmentEngine, SchedulerReport

# Импортируем твой СКОМПИЛИРОВАННЫЙ UI
from ui_main_window import Ui_MainWindow


# -----------------------------------------------------------------
# 3. HELPER-КЛАСС ДЛЯ PANDAS (вспомогательный)
# -----------------------------------------------------------------
class PandasModel(QAbstractTableModel):
    """Класс-модель для интеграции Pandas DataFrame с QTableView."""

    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.DisplayRole:
            return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Vertical:
                return str(self._data.index[section])
        return None


# -----------------------------------------------------------------
# 4. ГЛАВНЫЙ КЛАСС ПРИЛОЖЕНИЯ (ТВОЯ РАБОТА)
# -----------------------------------------------------------------
class AppWindow(QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()

        self.setupUi(self)

        # --- Настройка виджета выбора даты ---
        self.week_date_edit.setCalendarPopup(True)
        self.week_date_edit.setDisplayFormat("dd.MM.yyyy")

        # Устанавливаем дату по умолчанию на СЛЕДУЮЩИЙ понедельник
        today = QDate.currentDate()
        day_of_week = today.dayOfWeek()  # 1 = Пн, 7 = Вс

        # Магия расчета: (8 - 1 (Пн)) = 7 дней. (8 - 7 (Вс)) = 1 день.
        days_to_next_monday = 8 - day_of_week

        next_monday = today.addDays(days_to_next_monday)
        self.week_date_edit.setDate(next_monday)

        # 4.3. Загружаем "сырые" данные
        try:
            self.workers_df = pd.read_csv("workers.csv")
            self.equipment_df = pd.read_csv("equipment.csv")
            self.schedule_df = pd.read_csv("assignment_output_GUI.csv")
            self.requirements_df = pd.read_csv("position_requirements.csv")
            self.plan_df = pd.read_csv("plan.csv")
        except FileNotFoundError as e:
            QMessageBox.critical(
                self, "Ошибка загрузки", f"Не найден файл: {e.filename}"
            )

        self.final_assignments_df = None
        self.problem_brigades = None
        # self.target_week = None
        # self.shift_candidates = None
        # self.slots_day = None
        # self.slots_evening = None
        # self.slots_night = None
        # self.shift_equipment_day = None
        # self.shift_equipment_evening = None
        # self.shift_equipment_night = None

        # Подключение обработчиков событий для кнопок
        # Кнопка запуска генератора
        self.generate_button.clicked.connect(self.run_full_generation)
        # Кнопка сохранить
        self.save_button.clicked.connect(self.save_results_to_csv)
        # Показать список работников
        self.view_workers_button.clicked.connect(self.view_workers)
        # Показать список оборудования
        self.view_equipment_button.clicked.connect(self.view_equipment)
        # Показать Исторический график
        self.view_history_button.clicked.connect(self.view_history)

        # Заглушки
        self.pre_assign_button.clicked.connect(self.show_stub_message)
        self.edit_button.clicked.connect(self.show_stub_message)

        # --- НАЧНИ ПИСАТЬ ЗДЕСЬ ---
        # Что нужно сделать в первую очередь, чтобы наш
        # скомпилированный UI (Ui_MainWindow) появился на экране?

    # --- Здесь будем писать наши методы-обработчики ---

    def run_full_generation(self):
        """Запускается по нажатию 'generate_button'."""
        # 1. Получаем полную дату (как объект QDate)
        selected_date = self.week_date_edit.date()
        # 2. Извлекаем из нее номер недели .weekNumber() возвращает (неделя, год)
        (target_week, _) = selected_date.weekNumber()
        # print(target_week)

        if target_week > 0:
            QMessageBox.information(self, "Генерация", "Генерация запущениа")
            required_cols = ["worker_id", "week", "shift"]
            pipeline = DataPipeline(
                self.workers_df,
                self.equipment_df,
                self.schedule_df[required_cols],
                self.requirements_df,
                self.plan_df,
            )
            pipeline.run(target_week)

            engine = AssignmentEngine(
                pipeline.shift_candidates,
                pipeline.shift_equipment_day,
                pipeline.shift_equipment_evening,
                pipeline.shift_equipment_night,
            )

            engine.run()

            scheduler_report = SchedulerReport(
                engine.shift_equipment_day,
                engine.shift_equipment_evening,
                engine.shift_equipment_night,
                self.workers_df,
                pipeline.shift_candidates,  # DF всех кандидатов
                engine.global_assigned,  # set() всех назначенных
                pipeline.plan_long,
            )

            scheduler_report.get_final_assignments()
            scheduler_report.get_brigade_summary()

            # Генерируем текстовый отчет
            scheduler_report.generate_text_summary(target_week)

            self.final_assignments_df = scheduler_report.final_assignments_df
            self.problem_brigades = scheduler_report.problem_brigades()

            # Cоздаем модель таблицы
            assignments_Tabl_all = PandasModel(scheduler_report.all_shifts)
            assignments_Tabl_night = PandasModel(
                scheduler_report.all_shifts[
                    scheduler_report.all_shifts["shift"] == "night"
                ]
            )
            assignments_Tabl_day = PandasModel(
                scheduler_report.all_shifts[
                    scheduler_report.all_shifts["shift"] == "day"
                ]
            )
            assignments_Tabl_evening = PandasModel(
                scheduler_report.all_shifts[
                    scheduler_report.all_shifts["shift"] == "evening"
                ]
            )

            col = ["worker_id", "name", "primary_profession", "all_professions"]
            assignments_Tabl_no_position = PandasModel(engine.no_position[col])

            assignments_Tabl_problem_brigades = PandasModel(self.problem_brigades)

            # 1. Устанавливаем модель в таблицу
            self.results_table.setModel(assignments_Tabl_all)
            self.results_table_night.setModel(assignments_Tabl_night)
            self.results_table_day.setModel(assignments_Tabl_day)
            self.results_table_evening.setModel(assignments_Tabl_evening)
            self.results_table_no_position.setModel(assignments_Tabl_no_position)
            self.problem_brigades_table.setModel(assignments_Tabl_problem_brigades)

            summary_model = QStringListModel(scheduler_report.summary_lines)
            self.summary_list.setModel(summary_model)

            # 2. Меняем размер колонок
            self.results_table.resizeColumnsToContents()
            self.results_table_night.resizeColumnsToContents()
            self.results_table_day.resizeColumnsToContents()
            self.results_table_evening.resizeColumnsToContents()

            QMessageBox.information(self, "Успех", "Генерация выполнена")

        else:
            QMessageBox.warning(
                self, "Ошибка", "Не верно задана неделя.\nДопустимые значения 1 - 53"
            )

    def show_stub_message(self):
        """Показывает сообщение, что функция не готова."""
        QMessageBox.warning(
            self,
            "В разработке",
            "Эта функция еще не реализована, но для нее есть кнопка!",
        )

    def view_workers(self):
        """Показывает работников."""
        # Cоздаем модель
        workers_Tabl = PandasModel(self.workers_df)

        # 1. Устанавливаем модель в таблицу
        self.data_view_table.setModel(workers_Tabl)

        # 2. Меняем размер колонок
        self.data_view_table.resizeColumnsToContents()

    def view_equipment(self):
        """Показывает оборудование."""
        # Cоздаем модель
        equipment_Tabl = PandasModel(self.equipment_df)

        # 1. Устанавливаем модель в таблицу
        self.data_view_table.setModel(equipment_Tabl)

        # 2. Меняем размер колонок
        self.data_view_table.resizeColumnsToContents()

    def view_history(self):
        """Показывает историю."""
        # Cоздаем модель
        schedule_Tabl = PandasModel(self.schedule_df)

        # 1. Устанавливаем модель в таблицу
        self.data_view_table.setModel(schedule_Tabl)

        # 2. Меняем размер колонок
        self.data_view_table.resizeColumnsToContents()

    def load_saved_results(self, file_path="assignment_output_GUI.csv"):
        """Перечитывает сохранённый CSV и обновляет таблицу и отчёт."""
        try:
            df = pd.read_csv(file_path)
            self.final_assignments_df = df

            # Обновляем текстовый отчёт
            if hasattr(self, "scheduler_report"):
                current_week = df["week"].max()
                self.scheduler_report.final_assignments_df = df
                self.scheduler_report.generate_text_summary(current_week)
                self.summary_listview.setModel(
                    QStringListModel(self.scheduler_report.summary_lines)
                )

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить данные:\n{e}")

    def save_results_to_csv(self):
        """Сохраняет сгенерированный график и обновляет буфер данных."""
        if self.final_assignments_df is not None:
            try:
                file_path = "assignment_output_GUI.csv"

                # Проверяем, есть ли уже данные за эту неделю
                if os.path.exists(file_path):
                    existing = pd.read_csv(file_path)
                    current_week = self.final_assignments_df["week"].iloc[0]
                    if current_week in existing["week"].unique():
                        reply = QMessageBox.question(
                            self,
                            "Подтверждение перезаписи",
                            f"Данные за неделю {current_week} уже есть. Перезаписать?",
                            QMessageBox.Yes | QMessageBox.No,
                        )
                        if reply == QMessageBox.No:
                            return
                        # Удаляем старые записи этой недели перед добавлением
                        existing = existing[existing["week"] != current_week]
                        df_to_save = pd.concat(
                            [existing, self.final_assignments_df], ignore_index=True
                        )
                    else:
                        df_to_save = pd.concat(
                            [existing, self.final_assignments_df], ignore_index=True
                        )
                else:
                    df_to_save = self.final_assignments_df.copy()

                # Сохраняем итоговый CSV
                df_to_save.to_csv(file_path, index=False, encoding="utf-8-sig")

                # ✅ После сохранения — обновляем буфер
                self.load_saved_results(file_path)

                QMessageBox.information(
                    self, "Успех", "Файл сохранен и данные обновлены."
                )

            except Exception as e:
                QMessageBox.critical(
                    self, "Ошибка сохранения", f"Не удалось сохранить файл:\n{e}"
                )
        else:
            QMessageBox.critical(self, "Ошибка", "Сначала сгенерируйте график!")


def set_dark_palette(app):
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(43, 43, 43))
    palette.setColor(QPalette.WindowText, QColor(220, 220, 220))
    palette.setColor(QPalette.Base, QColor(30, 30, 30))
    palette.setColor(QPalette.AlternateBase, QColor(43, 43, 43))
    palette.setColor(QPalette.ToolTipBase, QColor(255, 255, 220))
    palette.setColor(QPalette.ToolTipText, QColor(0, 0, 0))
    palette.setColor(QPalette.Text, QColor(220, 220, 220))
    palette.setColor(QPalette.Button, QColor(43, 43, 43))
    palette.setColor(QPalette.ButtonText, QColor(220, 220, 220))
    palette.setColor(QPalette.BrightText, QColor(255, 0, 0))
    palette.setColor(QPalette.Link, QColor(42, 130, 218))
    palette.setColor(QPalette.Highlight, QColor(80, 80, 80))
    palette.setColor(QPalette.HighlightedText, QColor(90, 120, 200))
    app.setPalette(palette)


# -----------------------------------------------------------------
# 5. ЗАПУСК ПРИЛОЖЕНИЯ (Этот код не меняй)
# -----------------------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    set_dark_palette(app)
    window = AppWindow()
    window.show()
    sys.exit(app.exec_())
