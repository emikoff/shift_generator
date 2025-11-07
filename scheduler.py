import pandas as pd


class DataPipeline:
    """_summary_

    Returns:
        _type_: _description_
    """

    def __init__(self, workers, equipment, schedule, requirements, plan):
        # Загрузка датафреймов
        self.workers = workers
        self.equipment = equipment
        self.schedule = schedule
        self.requirements = requirements
        self.plan = plan

        # Декларация будущих данных
        self.plan_long = None
        self.shift_candidates = None
        self.shift_equipment_day = None
        self.shift_equipment_evening = None
        self.shift_equipment_night = None

        # Запускаем подготовку данных
        # - self.workers определяет основную  и смежные професии
        # - self.plan_long -> self.plan_long
        #   1 строка = machine_id, shift, machine_type, week
        self._prepare_base_data()
        # print("DataPipeline: Базовые данные подготовлены.")

    def _prepare_base_data(self):
        """
        Выполняет универсальную подготовку данных (добавление профессий,
        создание 'plan_long').
        """
        # --- Блок 1: Подготовка self.workers
        # Определяем основную профессию
        cols = ["flat_printing", "letterpress_printing", "inkjet_printing"]
        self.workers["primary_profession"] = self.workers[cols].idxmax(axis=1)

        # Добавляем все професии работника
        self.workers["all_professions"] = self.workers.apply(
            lambda row: [c for c in cols if row[c] > 0], axis=1
        )

        # --- Блок 2: Подготовка self.plan
        # Преобразуем в длинный формат
        plan_long = self.plan.melt(
            id_vars="machine_id",
            value_vars=["night", "day", "evening"],
            var_name="shift",
            value_name="works",
        )

        # Оставляем только те строки, где машина работает
        plan_long = (
            plan_long[plan_long["works"] == True]
            .drop(columns="works")
            .reset_index(drop=True)
        )

        plan_long = plan_long.merge(
            self.equipment[["machine_id", "machine_type"]], on="machine_id", how="left"
        )

        self.plan_long = plan_long

        # Позже удалить
        # print("prepare_data() завершен.")

    def _build_shift_rotation(self, target_week) -> pd.DataFrame:
        """
        Формирует общий датафрейм кандидатов на target_week для всех смен сразу,
        применяя правило переворота смен:
            Ночь -> Вечер, День -> Ночь, Вечер -> День
        """
        shift_map = {"night": "evening", "day": "night", "evening": "day"}

        # Базовый слой — прошлая неделя
        prev = self.schedule.loc[self.schedule["week"] == target_week - 1].copy()

        # Сохраним прошлую смену (на всякий случай для анализа)
        prev = prev.rename(columns={"shift": "prev_shift"})

        # Рассчитаем смену на следующую неделю и проставим неделю
        prev["shift"] = prev["prev_shift"].map(shift_map)
        prev["week"] = target_week

        # Объединим с данными по работникам
        self.shift_candidates = prev.merge(self.workers, on="worker_id", how="left")

        # Позже удалить
        # Короткая статистика по сменам
        counts = self.shift_candidates["shift"].value_counts()
        total = int(counts.sum())
        # print(f"Кандидаты на работу в неделю {target_week}: всего {total}")
        # for s in ["day", "evening", "night"]:
        # print(f"  {s}: {int(counts.get(s, 0))}")

    def _create_shift_slots(self, shif_name, target_week):
        """
        Вспомогательный метод. Создает пустые слоты для одной смены
        на основе self.plan_long и self.requirements.
        Возвращает: shift_slots пустые слоты потребности в рабочих
        """
        # Добавляем 'week' к self.plan_long
        plan_for_week = self.plan_long.copy()
        plan_for_week["week"] = target_week

        shift_slots = plan_for_week[plan_for_week["shift"] == shif_name][
            ["week", "shift", "machine_id", "machine_type"]
        ]

        shift_slots = shift_slots.merge(
            self.requirements, on="machine_type", how="left"
        )
        shift_slots["worker_id"] = None

        return shift_slots

    def run(self, target_week):
        # Получаем кандидатов для работы на целевую неделю
        self._build_shift_rotation(target_week)

        # Создаем пустые слоты (потребности)
        self.shift_equipment_day = self._create_shift_slots("day", target_week)
        self.shift_equipment_evening = self._create_shift_slots("evening", target_week)
        self.shift_equipment_night = self._create_shift_slots("night", target_week)


class AssignmentEngine:
    def __init__(
        self,
        shift_candidates,
        shift_equipment_day,
        shift_equipment_evening,
        shift_equipment_night,
    ):
        """
        Конструктор класса. Загружает данные и выполняет
        первичную, не зависящую от недели, подготовку.
        """
        # Загрузка датафреймов
        self.shift_candidates = shift_candidates
        self.shift_equipment_day = shift_equipment_day
        self.shift_equipment_evening = shift_equipment_evening
        self.shift_equipment_night = shift_equipment_night

        # Декларация будущих атрибутов
        self.global_assigned = set()
        self.assigned_day = set()
        self.assigned_evening = set()
        self.assigned_night = set()
        self.all_shifts = None

    def _find_candidates(self, assigned_shift, mode, profession, min_rank, shift_name):
        """
        Ядро алгоритма: ищет подходящих кандидатов на позицию.
        """
        assigned_shift = set(assigned_shift)
        # self.global_assigned = set(global_assigned or set())
        blocked_ids = self.global_assigned - assigned_shift

        base_mask = (
            (self.shift_candidates["shift"] == shift_name)
            & (~self.shift_candidates["worker_id"].isin(blocked_ids))
            & (~self.shift_candidates["worker_id"].isin(assigned_shift))
        )

        profession_mask = self.shift_candidates["all_professions"].apply(
            lambda profs: profession in profs
        )

        if mode == "ferst":
            primary_mask = self.shift_candidates["primary_profession"] == profession
            rank_mask = self.shift_candidates[profession] == min_rank
            candidates = self.shift_candidates[base_mask & primary_mask & rank_mask]
        elif mode == "second":
            rank_mask = self.shift_candidates[profession].isin([min_rank, min_rank + 1])
            candidates = self.shift_candidates[base_mask & profession_mask & rank_mask]
        elif mode == "third":
            rank_mask = self.shift_candidates[profession] > 0
            candidates = self.shift_candidates[base_mask & profession_mask & rank_mask]
        else:
            raise ValueError(
                f"Неизвестный режим '{mode}'. Используйте 'ferst', 'second' или 'third'."
            )

        return candidates.sort_values(
            by=[profession, "worker_id"], ascending=[False, True]
        )

    def _fill_positions(
        self,
        shift_equipment,
        assigned_shift,
        mode="ferst",
        shift_name="day",
    ):
        free_positions = []
        updated = shift_equipment.copy()

        for i, row in updated.iterrows():
            worker_id = row.get("worker_id")
            if pd.isna(worker_id) or worker_id in ("", None):
                profession = row["machine_type"]
                min_rank = row["min_rank"]

                candidates = self._find_candidates(
                    assigned_shift, mode, profession, min_rank, shift_name
                )

                if not candidates.empty:
                    chosen = candidates.iloc[0]
                    updated.loc[i, "worker_id"] = chosen["worker_id"]
                    assigned_shift.add(chosen["worker_id"])
                else:
                    free_positions.append(updated.loc[i])

        columns = updated.columns
        free_df = (
            pd.DataFrame(free_positions, columns=columns)
            if free_positions
            else pd.DataFrame(columns=columns)
        )

        return free_df, updated, assigned_shift

    def _run_assignment_for_shift(
        self, shift_equipment, assigned_shift, default_rounds
    ):

        fill_positions = self._fill_positions(
            shift_equipment,
            assigned_shift,
            mode=default_rounds[0][0],
            shift_name=default_rounds[0][1],
        )
        free_positions, updated, assigned_shift = fill_positions

        for round_idx, (mode, shift_name) in enumerate(default_rounds[1:], start=2):
            if free_positions.empty:
                break
            # print(
            #     f"Остались свободные позиции после тура {round_idx - 1}: {len(free_positions)}"
            # )
            fill_positions = self._fill_positions(
                free_positions,
                assigned_shift,
                mode=mode,
                shift_name=shift_name,
            )
            free_positions, patch, assigned_shift = fill_positions
            updated = updated.combine_first(patch)

        return updated, assigned_shift

    def _summary_team(self, shift_equipment):
        summary = shift_equipment.groupby(
            ["machine_id", "machine_type"], as_index=False
        ).agg(
            required=("position", "count"),
            assigned=("worker_id", lambda s: s.notna().sum() - (s == "").sum()),
        )
        return summary

    def _incomplete_team(self, shift_equipment):
        summary = self._summary_team(shift_equipment)
        incomplete = summary[
            (summary["assigned"] > 0) & (summary["assigned"] < summary["required"])
        ].copy()
        return incomplete

    def _decomlate_team(self, shift_equipment, assigned_shift):
        incomplete = self._incomplete_team(shift_equipment)
        destaff = incomplete[incomplete["required"] / 2 >= incomplete["assigned"]][
            "machine_id"
        ].to_list()

        if not destaff:
            return shift_equipment, assigned_shift

        mask = (
            shift_equipment["machine_id"].isin(destaff)
            & shift_equipment["worker_id"].notna()
        )
        freed = shift_equipment.loc[mask, "worker_id"].dropna().tolist()

        shift_equipment.loc[mask, "worker_id"] = None
        assigned_shift -= set(freed)

        # print(f"Расформированы бригады: {destaff}")
        return shift_equipment, assigned_shift

    def _staff_team(
        self, shift_equipment, assigned_shift, shift_name="night", mode="third"
    ):
        incomplete = self._incomplete_team(shift_equipment)
        if incomplete.empty:
            return shift_equipment, assigned_shift

        mask = shift_equipment["machine_id"].isin(incomplete["machine_id"])

        fill_positions = self._fill_positions(
            shift_equipment.loc[mask].copy(),
            assigned_shift,
            mode=mode,
            shift_name=shift_name,
        )
        _, patch, assigned_shift = fill_positions

        updated = shift_equipment.copy()
        if not patch.empty:
            updated.update(patch[["worker_id"]])

        return updated, assigned_shift

    def run(self):
        """
        Главный метод-дирижер. Запускает полный цикл
        планирования для 'target_week'.
        """
        # Конфигурация раундов назначение работников на позиции
        default_tourse = [
            ("ferst", "day"),
            ("second", "day"),
            ("ferst", "night"),
            ("second", "night"),
            ("ferst", "evening"),
            ("second", "evening"),
        ]
        default_tourse_day = default_tourse.copy()
        default_tourse_evening = default_tourse[4:] + default_tourse[:4]
        default_tourse_night = default_tourse[2:] + default_tourse[:2]

        # Генератор Дневной смены
        self.shift_equipment_day, self.assigned_day = self._run_assignment_for_shift(
            self.shift_equipment_day, self.assigned_day, default_tourse_day
        )
        self.shift_equipment_day, self.assigned_day = self._decomlate_team(
            self.shift_equipment_day, self.assigned_day
        )
        self.shift_equipment_day, self.assigned_day = self._staff_team(
            self.shift_equipment_day,
            self.assigned_day,
            shift_name="day",
        )
        self.global_assigned.update(self.assigned_day)

        # Генератор eveningней смены
        self.shift_equipment_evening, self.assigned_evening = (
            self._run_assignment_for_shift(
                self.shift_equipment_evening,
                self.assigned_evening,
                default_tourse_evening,
            )
        )
        self.shift_equipment_evening, self.assigned_evening = self._decomlate_team(
            self.shift_equipment_evening, self.assigned_evening
        )
        self.shift_equipment_evening, self.assigned_evening = self._staff_team(
            self.shift_equipment_evening,
            self.assigned_evening,
            shift_name="evening",
        )
        self.global_assigned.update(self.assigned_evening)

        # Генератор Ночной смены
        self.shift_equipment_night, self.assigned_night = (
            self._run_assignment_for_shift(
                self.shift_equipment_night, self.assigned_night, default_tourse_night
            )
        )
        self.shift_equipment_night, self.assigned_night = self._decomlate_team(
            self.shift_equipment_night, self.assigned_night
        )
        self.shift_equipment_night, self.assigned_night = self._staff_team(
            self.shift_equipment_night,
            self.assigned_night,
            shift_name="night",
        )
        self.global_assigned.update(self.assigned_night)


class SchedulerReport:
    def __init__(
        self,
        shift_equipment_night,
        shift_equipment_day,
        shift_equipment_evening,
        workers,
        shift_candidates,
        global_assigned_set,
    ):
        self.shift_equipment_night = shift_equipment_night
        self.shift_equipment_day = shift_equipment_day
        self.shift_equipment_evening = shift_equipment_evening
        self.workers = workers

        self.shift_candidates = shift_candidates
        self.global_assigned_set = global_assigned_set

        self.final_assignments_df = None
        self.report = None

        self.summary_lines = []

    def _summary_team(self, shift_equipment):
        summary = shift_equipment.groupby(
            ["machine_id", "machine_type"], as_index=False
        ).agg(
            required=("position", "count"),
            assigned=("worker_id", lambda s: s.notna().sum() - (s == "").sum()),
        )

        return summary

    def get_final_assignments(self):
        """
        Собирает все смены в один DataFrame и добавляет имена.
        """
        # Собираем все смены в один график night, day, evening
        self.all_shifts = pd.concat(
            [
                self.shift_equipment_night,
                self.shift_equipment_day,
                self.shift_equipment_evening,
            ],
            ignore_index=True,
        )
        # Добавляем персональные данные работникам
        self.all_shifts = self.all_shifts.merge(
            self.workers[["worker_id", "name"]],
            on="worker_id",
            how="left",
        )

        # Удаляем пустые позиции
        assigned_rows = self.all_shifts[self.all_shifts["worker_id"].notna()].copy()
        # Сортируем по смена, машина, позиция
        assigned_rows = assigned_rows.sort_values(
            by=["shift", "machine_id", "position"],
            ignore_index=True,
        )
        self.final_assignments_df = assigned_rows[
            ["week", "shift", "machine_id", "position", "worker_id", "name"]
        ]

    def get_unfilled_positions(self):
        """
        Возвращает DataFrame со всеми незаполненными позициями.
        """
        all_shifts = pd.concat(
            [
                self.shift_equipment_night,
                self.shift_equipment_day,
                self.shift_equipment_evening,
            ],
            ignore_index=True,
        )
        self.final_assignments_df = all_shifts[all_shifts["worker_id"].isna()]

    def get_brigade_summary(self):
        """
        Возвращает сводку по всем бригадам.
        """

        all_shifts = pd.concat(
            [
                self.shift_equipment_night,
                self.shift_equipment_day,
                self.shift_equipment_evening,
            ],
            ignore_index=True,
        )

        self.report = self._summary_team(all_shifts)

    def generate_text_summary(self, target_week):
        """
        Генерирует текстовый отчет (список строк) для QListView.
        ПРИМЕЧАНИЕ: Должен вызываться ПОСЛЕ get_brigade_summary()
        """
        self.summary_lines = []

        if self.report is None:
            self.summary_lines.append(
                "Ошибка: Сводка по бригадам (self.report) не создана."
            )
            self.summary_lines.append(
                "Вызовите get_brigade_summary() перед этим методом."
            )
            return

        try:
            # --- Блок 1: Работники ---
            total_available = len(self.shift_candidates)
            total_assigned = len(self.global_assigned_set)
            total_unassigned = total_available - total_assigned

            self.summary_lines.append("--- РАБОТНИКИ ---")
            self.summary_lines.append(f"Целевая неделя: {target_week}")
            self.summary_lines.append(f"Всего доступно: {total_available}")
            self.summary_lines.append(f"Назначено на смены: {total_assigned}")
            self.summary_lines.append(f"Остались без смены: {total_unassigned}")
            self.summary_lines.append("")  # Пустая строка

            # --- Блок 2: Позиции (Слоты) ---
            # self.report - это DataFrame, созданный в get_brigade_summary()
            total_required = self.report["required"].sum()
            total_filled = self.report["assigned"].sum()
            total_empty = total_required - total_filled

            self.summary_lines.append("--- ПОЗИЦИИ (СЛОТЫ) ---")
            self.summary_lines.append(f"Всего требуется позиций: {int(total_required)}")
            self.summary_lines.append(f"Заполнено позиций: {int(total_filled)}")
            self.summary_lines.append(f"Осталось вакантных: {int(total_empty)}")
            self.summary_lines.append("")

            # --- Блок 3: Проблемные бригады ---
            self.summary_lines.append("--- !!! ПРОБЛЕМНЫЕ БРИГАДЫ ---")

            # Находим неполные бригады
            incomplete_df = self.report[
                (self.report["assigned"] < self.report["required"])
                & (self.report["assigned"] > 0)
            ]
            # Находим пустые бригады
            empty_df = self.report[self.report["assigned"] == 0]
            # Находим полные бригады
            full_df = self.report[self.report["assigned"] == self.report["required"]]

            self.summary_lines.append(f"Всего бригад в плане: {len(self.report)}")
            self.summary_lines.append(f"Укомплектовано (N/N): {len(full_df)}")
            self.summary_lines.append(f"Неукомплектовано (M/N): {len(incomplete_df)}")
            self.summary_lines.append(f"Не запущено (0/N): {len(empty_df)}")

            if not incomplete_df.empty:
                self.summary_lines.append("")
                self.summary_lines.append("Список неполных (Назн/Треб):")
                # Сортируем по самому большому недобору
                incomplete_df = incomplete_df.sort_values(by="assigned")
                for _, row in incomplete_df.iterrows():
                    self.summary_lines.append(
                        f"  - {row['machine_id']}: {int(row['assigned'])} из {int(row['required'])}"
                    )

        except Exception as e:
            self.summary_lines = ["Ошибка при генерации отчета:", str(e)]
