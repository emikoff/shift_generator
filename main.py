"""PyQt5 desktop front-end for the shift scheduler toolchain."""

# pyuic5 ui_main_window.ui -o ui_main_window.py
import sys
import os
import pandas as pd

# -----------------------------------------------------------------
# 1. ИМПОРТЫ QT (Используем PyQt5)
# -----------------------------------------------------------------
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from PyQt5.QtCore import QAbstractTableModel, Qt, QDate, QStringListModel

# Импорт для темной темы
from PyQt5.QtGui import QPalette, QColor

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
        """Сохраняет DataFrame, который будем отображать в Qt."""
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        """Возвращает количество строк исходного DataFrame."""
        return self._data.shape[0]

    def columnCount(self, parent=None):
        """Возвращает количество колонок исходного DataFrame."""
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        """Форматирует ячейку в строку для отображения в таблице."""
        if not index.isValid():
            return None
        if role == Qt.DisplayRole:
            return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        """Отдаёт заголовки столбцов/строк из исходного DataFrame."""
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Vertical:
                return str(self._data.index[section])
        return None


class AppWindow(QMainWindow, Ui_MainWindow):
    """Главное окно приложения: загружает данные и управляет GUI."""

    def __init__(self):
        """Подготавливает UI, дату по умолчанию и исходные DataFrame."""
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
            self.workers_df = pd.read_csv("data/workers.csv")
            self.equipment_df = pd.read_csv("data/equipment.csv")
            self.schedule_df = pd.read_csv("data/assignment_history.csv")
            self.requirements_df = pd.read_csv("data/position_requirements.csv")
            self.plan_df = pd.read_csv("data/plan.csv")
        except FileNotFoundError as e:
            QMessageBox.critical(
                self, "Ошибка загрузки", f"Не найден файл: {e.filename}"
            )

        self.final_assignments_df = None
        self.problem_brigades = None
        self.scheduler_report = None
        self._table_models = {}
        self.summary_model = None

        # Подключение обработчиков событий для кнопок
        self.generate_button.clicked.connect(self.run_full_generation)
        self.save_button.clicked.connect(self.save_results_to_csv)
        self.view_workers_button.clicked.connect(self.view_workers)
        self.view_equipment_button.clicked.connect(self.view_equipment)
        self.view_history_button.clicked.connect(self.view_history)
        self.view_plan_button.clicked.connect(self.view_plan)

        # Заглушки
        # self.pre_assign_button.clicked.connect(self.show_stub_message)
        # self.edit_button.clicked.connect(self.show_stub_message)

    def _display_dataframe(self, table_widget, dataframe):
        """Создает модель и привязывает DataFrame к QTableView."""
        model = PandasModel(dataframe)
        table_widget.setModel(model)
        key = table_widget.objectName() or str(id(table_widget))
        self._table_models[key] = model
        table_widget.resizeColumnsToContents()
        return model

    def run_full_generation(self):
        """Запускает полный цикл генерации расписания."""
        selected_date = self.week_date_edit.date()
        (target_week, _) = selected_date.weekNumber()

        if target_week > 0:
            QMessageBox.information(self, "Генерация", "Генерация запущена")
            # Pipeline использует только идентификаторы и смену
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
            self.scheduler_report = scheduler_report

            self.final_assignments_df = scheduler_report.final_assignments_df
            self.problem_brigades = scheduler_report.problem_brigades()

            # Cоздаем модель таблицы
            self._display_dataframe(self.results_table, scheduler_report.all_shifts)
            self._display_dataframe(
                self.results_table_night,
                scheduler_report.all_shifts[
                    scheduler_report.all_shifts["shift"] == "night"
                ],
            )
            self._display_dataframe(
                self.results_table_day,
                scheduler_report.all_shifts[
                    scheduler_report.all_shifts["shift"] == "day"
                ],
            )
            self._display_dataframe(
                self.results_table_evening,
                scheduler_report.all_shifts[
                    scheduler_report.all_shifts["shift"] == "evening"
                ],
            )

            col = ["worker_id", "name", "primary_profession", "all_professions"]
            self._display_dataframe(
                self.results_table_no_position, engine.no_position[col]
            )
            self._display_dataframe(self.problem_brigades_table, self.problem_brigades)

            summary_model = QStringListModel(scheduler_report.summary_lines)
            self.summary_list.setModel(summary_model)
            self.summary_model = summary_model

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
        self._display_dataframe(self.data_view_table, self.workers_df)

    def view_equipment(self):
        """Показывает оборудование."""
        self._display_dataframe(self.data_view_table, self.equipment_df)

    def view_history(self):
        """Показывает историю."""
        self._display_dataframe(self.data_view_table, self.schedule_df)

    def view_plan(self):
        """Показывает производственный план."""
        self._display_dataframe(self.data_view_table, self.plan_df)

    def load_saved_results(self, file_path="assignment_output_GUI.csv"):
        """Перечитывает сохранённый CSV и обновляет таблицу и отчёт."""
        try:
            df = pd.read_csv(file_path)
            self.final_assignments_df = df

            # Обновляем текстовый отчёт
            if isinstance(self.scheduler_report, SchedulerReport):
                current_week = df["week"].max()
                self.scheduler_report.final_assignments_df = df
                self.scheduler_report.generate_text_summary(current_week)
                summary_model = QStringListModel(self.scheduler_report.summary_lines)
                self.summary_list.setModel(summary_model)
                self.summary_model = summary_model

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить данные:\n{e}")

    def save_results_to_csv(self):
        """
        Сохраняет сгенерированный график (CSV + TXT)
        и обновляет буфер данных.
        """
        if self.final_assignments_df is not None:
            try:
                # --- Блок 1: Сохранение CSV (без изменений) ---
                file_path_csv = "assignment_output_GUI.csv"
                current_week = self.final_assignments_df["week"].iloc[0]

                if os.path.exists(file_path_csv):
                    existing = pd.read_csv(file_path_csv)

                    if current_week in existing["week"].unique():
                        reply = QMessageBox.question(
                            self,
                            "Подтверждение перезаписи",
                            f"Данные за неделю {current_week} уже есть. "
                            "Перезаписать их в assignment_output_GUI.csv?",
                            QMessageBox.Yes | QMessageBox.No,
                        )
                        if reply == QMessageBox.No:
                            return

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

                df_to_save.to_csv(file_path_csv, index=False, encoding="utf-8-sig")

                # --- Блок 2: (ИЗМЕНЕН) Сохранение .TXT файла ---
                file_path_txt = f"output/Расписание_Неделя_{current_week}.txt"

                # Получаем дату из GUI и конвертируем в стандартный тип Python
                start_date_py = self.week_date_edit.date().toPyDate()

                # Вызываем метод из SchedulerReport
                txt_content = self.scheduler_report.generate_human_readable_txt(
                    current_week, start_date_py
                )

                saved_txt_path = None
                if txt_content:
                    try:
                        with open(file_path_txt, "w", encoding="utf-8") as f:
                            f.write(txt_content)
                        saved_txt_path = file_path_txt
                    except Exception as txt_e:
                        print(f"Не удалось сохранить TXT файл: {txt_e}")

                # --- Блок 3: Обновление буфера (без изменений) ---
                self.load_saved_results(file_path_csv)

                # --- Блок 4: Сообщение об успехе (без изменений) ---
                msg = f"Файл CSV сохранен:\n{file_path_csv}\n\n"
                if saved_txt_path:
                    msg += f"Файл TXT сохранен:\n{saved_txt_path}\n\n"
                else:
                    msg += "Читаемый TXT файл НЕ сохранен (ошибка генерации).\n\n"
                msg += "Данные в программе обновлены."

                QMessageBox.information(self, "Успех", msg)

            except Exception as e:
                QMessageBox.critical(
                    self, "Ошибка сохранения", f"Не удалось сохранить файлы:\n{e}"
                )
        else:
            QMessageBox.critical(self, "Ошибка", "Сначала сгенерируйте график!")


def set_dark_palette(app):
    """Включает простую темную тему для всего приложения."""
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
