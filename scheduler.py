import pandas as pd
from datetime import timedelta


class DataPipeline:
    """Готовит рабочие DataFrame для целевой недели."""

    def __init__(self, workers, equipment, schedule, requirements, plan):
        """
        Args:
            workers: DataFrame с персоналом и их навыками.
            equipment: DataFrame с оборудованием и типами машин.
            schedule: Исторический график (минимум worker_id/week/shift).
            requirements: Требования по минимальному рангу и численности.
            plan: План запуска машин по сменам.
        """
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
            id_vars=["machine_id", "week"],
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

    def _create_shift_slots(self, shift_name, target_week):
        """
        Формирует слоты для одной смены на основе plan_long + requirements.
        """
        plan_for_week = self.plan_long[self.plan_long["week"] == target_week].copy()

        shift_slots = plan_for_week[plan_for_week["shift"] == shift_name][
            ["week", "shift", "machine_id", "machine_type"]
        ]

        shift_slots = shift_slots.merge(
            self.requirements, on="machine_type", how="left"
        )
        shift_slots["worker_id"] = None

        return shift_slots

    def run(self, target_week):
        """Вычисляет кандидатов и слоты под целевую неделю."""
        self._build_shift_rotation(target_week)

        self.shift_equipment_day = self._create_shift_slots("day", target_week)
        self.shift_equipment_evening = self._create_shift_slots("evening", target_week)
        self.shift_equipment_night = self._create_shift_slots("night", target_week)


class AssignmentEngine:
    """Ищет исполнителей по слотам и фиксирует глобальные назначения."""

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
        self.no_position = None

    def _find_candidates(self, assigned_shift, mode, profession, min_rank, shift_name):
        """
        Ядро алгоритма: ищет подходящих кандидатов на позицию.
        """
        assigned_shift = set(assigned_shift)
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
        """Пытается закрыть слоты для конкретной смены и фиксирует свободные позиции."""
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
        """Запускает серию туров (_fill_positions) согласно конфигурации default_rounds."""

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
        """Считает требуемые и назначенные позиции для каждой машины."""
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
        """Расформировывает бригады, где назначено меньше половины от требуемого."""
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

        self.no_position = self.shift_candidates[
            ~self.shift_candidates["worker_id"].isin(self.global_assigned)
        ]


class SchedulerReport:
    """Формирует итоговые таблицы и текстовые отчёты по расписанию."""

    def __init__(
        self,
        shift_equipment_night,
        shift_equipment_day,
        shift_equipment_evening,
        workers,
        shift_candidates,
        global_assigned_set,
        plan_long,
    ):
        """Сохраняет ссылки на результаты движка назначения и справочники."""
        self.shift_equipment_night = shift_equipment_night
        self.shift_equipment_day = shift_equipment_day
        self.shift_equipment_evening = shift_equipment_evening
        self.workers = workers

        self.shift_candidates = shift_candidates
        self.global_assigned_set = global_assigned_set
        self.plan_long = plan_long

        self.final_assignments_df = None
        self.report = None

        self.summary_lines = []

    def _combined_shifts(self):
        """Возвращает DataFrame со всеми сменами."""
        return pd.concat(
            [
                self.shift_equipment_night,
                self.shift_equipment_day,
                self.shift_equipment_evening,
            ],
            ignore_index=True,
        )

    def _summary_team(self, df, group_cols=None):
        """
        Универсальная сводка укомплектованности.
        group_cols: список ключей группировки (по умолчанию ['machine_id','machine_type']).
        """
        if group_cols is None:
            group_cols = ["machine_id", "machine_type"]

        summary = df.groupby(group_cols, as_index=False).agg(
            required=("position", "count"),
            assigned=("worker_id", lambda s: s.notna().sum() - (s == "").sum()),
        )
        # self.report = summary
        return summary

    def _incomplete_brigades(self):
        """Неполные (k/N, k>0). Источник: self.all_shifts (должен быть собран)."""
        rep = self._summary_team(
            self.all_shifts, ["week", "shift", "machine_id", "machine_type"]
        )
        df = rep[(rep["assigned"] > 0) & (rep["assigned"] < rep["required"])].copy()
        df["missing"] = df["required"] - df["assigned"]
        df["status"] = "incomplete"
        return df.sort_values(
            ["week", "shift", "missing", "machine_id"],
            ascending=[True, True, False, True],
        ).reset_index(drop=True)

    def _empty_brigades(self):
        """Пустые (0/N). Источник: self.all_shifts."""
        rep = self._summary_team(
            self.all_shifts, ["week", "shift", "machine_id", "machine_type"]
        )
        df = rep[(rep["assigned"] == 0) & (rep["required"] > 0)].copy()
        df["missing"] = df["required"]
        df["status"] = "empty"
        return df.sort_values(
            ["week", "shift", "required", "machine_id"],
            ascending=[True, True, False, True],
        ).reset_index(drop=True)

    def get_final_assignments(self):
        """
        Собирает все смены в один DataFrame и добавляет имена.
        """
        # Собираем все смены в один график night, day, evening
        self.all_shifts = self._combined_shifts()
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
        all_shifts = self._combined_shifts()
        self.final_assignments_df = all_shifts[all_shifts["worker_id"].isna()]

    def get_brigade_summary(self):
        """
        Возвращает сводку по всем бригадам.
        """

        self.report = self._summary_team(
            self._combined_shifts(), ["week", "shift", "machine_id", "machine_type"]
        )

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
            self.summary_lines.append("")

            # --- Блок 2: Позиции (Слоты) — считаем строго за target_week ---
            rep = self.report
            if "week" in rep.columns:
                rep = rep[rep["week"] == target_week].copy()

            total_required = int(rep["required"].sum())
            total_filled = int(rep["assigned"].sum())
            total_empty = total_required - total_filled

            self.summary_lines.append("--- ПОЗИЦИИ (СЛОТЫ) ---")
            self.summary_lines.append(f"Всего требуется позиций: {total_required}")
            self.summary_lines.append(f"Заполнено позиций: {total_filled}")
            self.summary_lines.append(f"Осталось вакантных: {total_empty}")
            self.summary_lines.append("")

            # --- Блок 3: Проблемные бригады ---
            self.summary_lines.append("--- !!! ПРОБЛЕМНЫЕ БРИГАДЫ ---")

            # 3.1. Сколько бригад ДОЛЖНО быть по плану за неделю
            planned_cnt = None
            if hasattr(self, "plan_long") and isinstance(self.plan_long, pd.DataFrame):
                pl = self.plan_long[self.plan_long["week"] == target_week].copy()
                if "works" in pl.columns:
                    # если столбец есть, фильтруем по нему
                    pl = pl[pl["works"].astype(int) == 1]
                # если столбца нет (как у тебя) — уже отфильтровано на этапе подготовки
                planned_cnt = (
                    pl[["week", "shift", "machine_id"]].drop_duplicates().shape[0]
                )
            else:
                # запасной путь: считаем по факту слотов
                planned_cnt = (
                    rep[["week", "shift", "machine_id"]].drop_duplicates().shape[0]
                )

            # 3.2. Фактическая укомплектованность (по rep за неделю)
            incomplete_df = rep[
                (rep["assigned"] > 0) & (rep["assigned"] < rep["required"])
            ]
            empty_df = rep[rep["assigned"] == 0]
            full_df = rep[rep["assigned"] == rep["required"]]

            self.summary_lines.append(
                f"Всего бригад в плане (week×shift×machine): {planned_cnt}"
            )
            self.summary_lines.append(f"Укомплектовано (N/N): {len(full_df)}")
            self.summary_lines.append(f"Неукомплектовано (M/N): {len(incomplete_df)}")
            self.summary_lines.append(f"Не запущено (0/N): {len(empty_df)}")

            if not incomplete_df.empty:
                self.summary_lines.append("")
                self.summary_lines.append("Список неполных (Назн/Треб):")
                inc = incomplete_df.assign(
                    missing=lambda d: d["required"] - d["assigned"]
                ).sort_values(
                    ["missing", "shift", "machine_id"], ascending=[False, True, True]
                )
                for _, row in inc.iterrows():
                    self.summary_lines.append(
                        f"  - нед.{int(row['week']):02d} {row['shift']}: "
                        f"{row['machine_id']} — {int(row['assigned'])} из {int(row['required'])}"
                    )

            # --- Список неназначённых (0/N) ---
            if not empty_df.empty:
                self.summary_lines.append("")
                self.summary_lines.append("Список неназначённых (0/Треб):")
                # сортируем: сперва наибольшая требуемая численность, затем смена и машина
                emp = empty_df.sort_values(
                    ["required", "shift", "machine_id"], ascending=[False, True, True]
                )
                has_week = "week" in emp.columns
                has_shift = "shift" in emp.columns
                for _, row in emp.iterrows():
                    prefix = ""
                    if has_week and has_shift:
                        prefix = f"нед.{int(row['week']):02d} {row['shift']}: "
                    self.summary_lines.append(
                        f"  - {prefix}{row['machine_id']} — 0 из {int(row['required'])}"
                    )

        except Exception as e:
            self.summary_lines = ["Ошибка при генерации отчета:", str(e)]

    def problem_brigades(self):
        """Возвращает объединённый список неполных и пустых бригад."""
        cols = [
            "week",
            "shift",
            "machine_id",
            "machine_type",
            "assigned",
            "required",
            "missing",
            "status",
        ]
        inc = self._incomplete_brigades()[cols]
        emp = self._empty_brigades()[cols]
        return (
            pd.concat([inc, emp], ignore_index=True)
            .sort_values(
                ["week", "shift", "status", "missing", "machine_id"],
                ascending=[True, True, True, False, True],
            )
            .reset_index(drop=True)
        )

    def generate_human_readable_txt(self, target_week, start_date):
        """
        Генерирует текстовое представление расписания для человека.
        (Вызывается из GUI)

        Args:
            target_week (int): Номер целевой недели.
            start_date (datetime.date): Объект date понедельника.
        """
        try:
            # 1. Фильтруем DF только по текущей неделе
            current_week_df = self.final_assignments_df[
                self.final_assignments_df["week"] == target_week
            ]

            if current_week_df.empty:
                return None  # Нет данных для генерации

            # 2. Собираем текстовые строки
            lines = []

            # --- Строка 1: Диапазон дат ---
            end_date = start_date + timedelta(days=4)  # Пятница
            date_range_str = (
                f"Расписание на неделю: {start_date.strftime('%d.%m.%Y')} - "
                f"{end_date.strftime('%d.%m.%Y')} (Неделя {target_week})"
            )

            lines.append(date_range_str)
            lines.append("=" * len(date_range_str))
            lines.append("")  # Пустая строка

            # 3. Цикл по сменам, машинам, позициям
            shift_translation = {"night": "Ночь", "day": "День", "evening": "Вечер"}
            shifts_order = ["night", "day", "evening"]

            for shift in shifts_order:
                shift_name = shift_translation.get(shift, shift.capitalize())
                lines.append(f"--- СМЕНА: {shift_name} ---")

                shift_df = current_week_df[
                    current_week_df["shift"] == shift
                ].sort_values(by=["machine_id", "position"])

                if shift_df.empty:
                    lines.append("\t(Нет назначений в этой смене)")
                    lines.append("")
                    continue

                machines = shift_df["machine_id"].unique()
                for machine in machines:
                    lines.append(f"\tМашина: {machine}")

                    machine_df = shift_df[shift_df["machine_id"] == machine]

                    for _, assignment_row in machine_df.iterrows():
                        pos_name = assignment_row["position"]
                        worker_name = assignment_row["name"]

                        if pd.isna(worker_name):
                            worker_name = "--- ВАКАНСИЯ ---"

                        lines.append(f"\t\t- Позиция {pos_name}: {worker_name}")

                    lines.append("")  # Пустая строка после каждой машины

            return "\n".join(lines)

        except Exception as e:
            print(f"Ошибка при генерации TXT в SchedulerReport: {e}")
            return None
