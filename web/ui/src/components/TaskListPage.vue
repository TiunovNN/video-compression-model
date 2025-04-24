<template>
    <div class="task-list-container">
        <h1>Список задач по кодированию видео</h1>

        <!-- Фильтры -->
        <div class="filter-container">
            <h2>Фильтры</h2>

            <div class="dropdown-filter">
                <div class="dropdown-header" @click="toggleFilterDropdown">
                    <span>Статусы: {{ getSelectedStatusesLabel() }}</span>
                    <i class="dropdown-arrow" :class="{ 'open': isFilterDropdownOpen }">▼</i>
                </div>

                <div class="dropdown-menu" v-show="isFilterDropdownOpen">
                    <div class="dropdown-item" v-for="status in availableStatuses" :key="status">
                        <input type="checkbox" :id="status" :value="status" v-model="selectedStatuses"
                            @change="updateTasks">
                        <label :for="status" :class="`status-label ${status}`">{{ getStatusLabel(status) }}</label>
                    </div>

                    <div class="dropdown-actions">
                        <button class="select-all-btn" @click="selectAllStatuses">Выбрать все</button>
                        <button class="clear-filter-btn" @click="clearFilters">Сбросить</button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Таблица задач -->
        <div class="tasks-table-container">
            <div v-if="loading" class="loading-indicator">
                <div class="spinner"></div>
                <p>Загрузка задач...</p>
            </div>

            <div v-else-if="error" class="error-message">
                <h3>Ошибка загрузки задач</h3>
                <p>{{ error }}</p>
                <button @click="updateTasks" class="retry-btn">Повторить</button>
            </div>

            <div v-else-if="tasks.length === 0" class="no-tasks">
                <p>Задачи не найдены. Измените параметры фильтрации или создайте новую задачу.</p>
                <button @click="goToCreateTask" class="create-task-btn">Создать задачу</button>
            </div>

            <table v-else class="tasks-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Исходный файл</th>
                        <th>Выходной файл</th>
                        <th>Статус</th>
                        <th>Создана</th>
                        <th>Обновлена</th>
                        <th>Действия</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="task in tasks" :key="task.id">
                        <td>{{ task.id }}</td>
                        <td class="file-cell">{{ getFileName(task.source_file) }}</td>
                        <td class="file-cell">{{ task.output_file ? getFileName(task.output_file) : '-' }}</td>
                        <td>
                            <span :class="`status-badge ${task.status}`">
                                {{ getStatusLabel(task.status) }}
                            </span>
                            <div v-if="task.error_message" class="error-tooltip">
                                <i class="error-icon">!</i>
                                <span class="error-tooltip-text">{{ task.error_message }}</span>
                            </div>
                        </td>
                        <td>{{ formatDate(task.created_at) }}</td>
                        <td>{{ formatDate(task.updated_at) }}</td>
                        <td class="actions-cell">
                            <button @click="viewTaskDetails(task.id)" class="action-btn view-btn">
                                Просмотр
                            </button>
                        </td>
                    </tr>
                </tbody>
            </table>

            <!-- Пагинация -->
            <div class="pagination">
                <button @click="prevPage" :disabled="currentPage <= 1" class="pagination-btn">
                    &laquo; Назад
                </button>

                <span class="page-info">
                    Страница {{ currentPage }} из {{ totalPages || 1 }}
                </span>

                <button @click="nextPage" :disabled="isLastPage" class="pagination-btn">
                    Вперед &raquo;
                </button>
            </div>

            <div class="page-size-control">
                <label for="pageSize">Задач на странице:</label>
                <select id="pageSize" v-model.number="pageSize" @change="updateTasks">
                    <option value="10">10</option>
                    <option value="25">25</option>
                    <option value="50">50</option>
                    <option value="100">100</option>
                </select>
            </div>
        </div>
    </div>
</template>

<script>
export default {
    data() {
        return {
            tasks: [],
            loading: true,
            error: null,
            // Статусы из спецификации API
            availableStatuses: ['pending', 'processing', 'completed', 'failed'],
            selectedStatuses: [],
            // Пагинация
            pageSize: 25,
            currentPage: 1,
            totalCount: 0,
            // Для определения общего количества страниц
            totalPages: 0,
            isFilterDropdownOpen: false,
        };
    },
    computed: {
        // Вычисляем, является ли текущая страница последней
        isLastPage() {
            return this.currentPage >= this.totalPages;
        },
        // Вычисляем смещение для API запроса
        skipCount() {
            return (this.currentPage - 1) * this.pageSize;
        }
    },
    created() {
        // Загружаем задачи при создании компонента
        this.updateTasks();
    },
    inject: ['apiURI'],
    methods: {
        // Получение списка задач с учетом фильтрации и пагинации
        async updateTasks() {
            this.loading = true;
            this.error = null;

            try {
                // Формируем параметры запроса
                const params = new URLSearchParams();
                params.append('limit', this.pageSize);
                params.append('skip', this.skipCount);

                // Добавляем выбранные статусы, если они есть
                if (this.selectedStatuses.length > 0) {
                    this.selectedStatuses.forEach(status => {
                        params.append('statuses', status);
                    });
                }

                // Выполняем запрос к API
                const response = await fetch(`${this.apiURI}/tasks?${params.toString()}`);

                if (!response.ok) {
                    throw new Error(`Ошибка сервера: ${response.status} ${response.statusText}`);
                }

                const data = await response.json();
                this.tasks = data.tasks;

                if (this.tasks.length < this.pageSize) {
                    this.totalCount = this.skipCount + this.tasks.length;
                } else {
                    this.totalCount = this.skipCount + this.pageSize + 1; // Предполагаем, что есть еще страницы
                }

                // Вычисляем общее количество страниц
                this.totalPages = Math.ceil(this.totalCount / this.pageSize);

            } catch (error) {
                console.error('Ошибка при загрузке задач:', error);
                this.error = error.message || 'Произошла ошибка при загрузке задач';
            } finally {
                this.loading = false;
            }
        },

        // Навигация по страницам
        prevPage() {
            if (this.currentPage > 1) {
                this.currentPage--;
                this.updateTasks();
            }
        },

        nextPage() {
            if (!this.isLastPage) {
                this.currentPage++;
                this.updateTasks();
            }
        },

        // Сброс фильтров
        clearFilters() {
            this.selectedStatuses = [];
            this.currentPage = 1;
            this.updateTasks();
        },

        // Отображение детальной информации о задаче
        viewTaskDetails(taskId) {
            this.$router.push({ path: `/view/${taskId}` });
        },

        // Переход к созданию новой задачи
        goToCreateTask() {
            this.$router.push({ path: '/create' });
        },

        // Форматирование даты и времени
        formatDate(dateString) {
            if (!dateString) return '-';

            try {
                const date = new Date(dateString);
                return new Intl.DateTimeFormat('ru-RU', {
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit'
                }).format(date);
            } catch (e) {
                console.error('Error formatting date:', e);
                return dateString;
            }
        },

        // Получение только имени файла из полного пути
        getFileName(path) {
            if (!path) return '';

            try {
                return path.split('/').pop();
            } catch (e) {
                return path;
            }
        },

        // Получение понятного названия статуса
        getStatusLabel(status) {
            const statusMap = {
                pending: 'Ожидает',
                processing: 'Обрабатывается',
                completed: 'Завершено',
                failed: 'Ошибка'
            };

            return statusMap[status] || status;
        },
        toggleFilterDropdown() {
            this.isFilterDropdownOpen = !this.isFilterDropdownOpen;
        },

        selectAllStatuses() {
            this.selectedStatuses = [...this.availableStatuses];
            this.updateTasks();
        },

        getSelectedStatusesLabel() {
            if (this.selectedStatuses.length === 0) {
                return "Все";
            } else if (this.selectedStatuses.length === this.availableStatuses.length) {
                return "Все";
            } else if (this.selectedStatuses.length === 1) {
                return this.getStatusLabel(this.selectedStatuses[0]);
            } else {
                return `Выбрано (${this.selectedStatuses.length})`;
            }
        },

        // Закрываем выпадающий список при клике вне его
        closeDropdownOnOutsideClick(event) {
            if (this.isFilterDropdownOpen && !this.$el.querySelector('.dropdown-filter').contains(event.target)) {
                this.isFilterDropdownOpen = false;
            }
        },
    }
};
</script>

<style scoped>
.task-list-container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
    font-family: Arial, sans-serif;
}

h1 {
    color: #333;
    text-align: center;
    margin-bottom: 30px;
}

h2 {
    color: #555;
    font-size: 18px;
    margin-bottom: 15px;
}

/* Стили для панели фильтров */
.filter-panel {
    background-color: #f5f5f5;
    border-radius: 6px;
    padding: 15px;
    margin-bottom: 20px;
    border: 1px solid #e0e0e0;
}

.status-filters {
    display: flex;
    flex-wrap: wrap;
    gap: 15px;
    margin-bottom: 15px;
}

.filter-item {
    display: flex;
    align-items: center;
}

.filter-item input[type="checkbox"] {
    margin-right: 5px;
}

.status-label {
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 14px;
}

.status-label.pending {
    background-color: #FFE0B2;
    color: #E65100;
}

.status-label.processing {
    background-color: #B3E0FF;
    color: #0277BD;
}

.status-label.completed {
    background-color: #C8E6C9;
    color: #2E7D32;
}

.status-label.failed {
    background-color: #FFCDD2;
    color: #C62828;
}

.clear-filter-btn {
    background-color: #f0f0f0;
    border: 1px solid #ccc;
    color: #333;
    padding: 6px 12px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.2s;
}

.clear-filter-btn:hover {
    background-color: #e0e0e0;
}

/* Стили для таблицы задач */
.tasks-table-container {
    background-color: white;
    border-radius: 6px;
    border: 1px solid #e0e0e0;
    overflow: hidden;
}

.tasks-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
}

.tasks-table th {
    background-color: #f5f5f5;
    padding: 12px 15px;
    text-align: left;
    border-bottom: 2px solid #ddd;
    font-weight: 600;
    color: #333;
}

.tasks-table td {
    padding: 10px 15px;
    border-bottom: 1px solid #e0e0e0;
    color: #555;
}

.tasks-table tr:hover {
    background-color: #f9f9f9;
}

.file-cell {
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.status-badge {
    display: inline-block;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 500;
}

.status-badge.pending {
    background-color: #FFE0B2;
    color: #E65100;
}

.status-badge.processing {
    background-color: #B3E0FF;
    color: #0277BD;
}

.status-badge.completed {
    background-color: #C8E6C9;
    color: #2E7D32;
}

.status-badge.failed {
    background-color: #FFCDD2;
    color: #C62828;
}

.actions-cell {
    white-space: nowrap;
}

.action-btn {
    padding: 5px 10px;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
    border: none;
    margin-right: 5px;
    transition: all 0.2s;
}

.view-btn {
    background-color: #E3F2FD;
    color: #1565C0;
}

.view-btn:hover {
    background-color: #BBDEFB;
}

.download-btn {
    background-color: #E8F5E9;
    color: #2E7D32;
}

.download-btn:hover {
    background-color: #C8E6C9;
}

/* Стили для ошибок */
.error-tooltip {
    position: relative;
    display: inline-block;
    margin-left: 8px;
}

.error-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 16px;
    height: 16px;
    background-color: #f44336;
    color: white;
    border-radius: 50%;
    font-size: 12px;
    font-style: normal;
    font-weight: bold;
    cursor: help;
}

.error-tooltip-text {
    visibility: hidden;
    width: 200px;
    background-color: #333;
    color: #fff;
    text-align: center;
    border-radius: 4px;
    padding: 8px;
    position: absolute;
    z-index: 1;
    bottom: 125%;
    left: 50%;
    transform: translateX(-50%);
    opacity: 0;
    transition: opacity 0.3s;
    font-size: 12px;
}

.error-tooltip:hover .error-tooltip-text {
    visibility: visible;
    opacity: 1;
}

/* Стили для пагинации */
.pagination {
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 15px;
    background-color: #f9f9f9;
    border-top: 1px solid #e0e0e0;
}

.pagination-btn {
    background-color: #fff;
    border: 1px solid #ddd;
    color: #333;
    padding: 8px 15px;
    margin: 0 5px;
    border-radius: 4px;
    cursor: pointer;
    transition: all 0.2s;
}

.pagination-btn:hover:not(:disabled) {
    background-color: #e9e9e9;
}

.pagination-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
}

.page-info {
    margin: 0 15px;
    font-size: 14px;
    color: #666;
}

.page-size-control {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    padding: 10px 15px;
    border-top: 1px solid #e0e0e0;
    font-size: 14px;
    color: #666;
}

.page-size-control label {
    margin-right: 10px;
}

.page-size-control select {
    padding: 5px;
    border-radius: 4px;
    border: 1px solid #ddd;
}

/* Стили для состояний загрузки и ошибок */
.loading-indicator {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 40px 20px;
    color: #666;
}

.spinner {
    border: 4px solid #f3f3f3;
    border-top: 4px solid #3498db;
    border-radius: 50%;
    width: 30px;
    height: 30px;
    animation: spin 1s linear infinite;
    margin-bottom: 15px;
}

@keyframes spin {
    0% {
        transform: rotate(0deg);
    }

    100% {
        transform: rotate(360deg);
    }
}

.error-message {
    padding: 30px;
    text-align: center;
    color: #d32f2f;
}

.retry-btn {
    background-color: #f44336;
    color: white;
    border: none;
    padding: 8px 15px;
    border-radius: 4px;
    margin-top: 15px;
    cursor: pointer;
    transition: background-color 0.2s;
}

.retry-btn:hover {
    background-color: #d32f2f;
}

.no-tasks {
    padding: 40px 20px;
    text-align: center;
    color: #666;
}

.create-task-btn {
    background-color: #4CAF50;
    color: white;
    border: none;
    padding: 10px 20px;
    border-radius: 4px;
    margin-top: 15px;
    cursor: pointer;
    font-size: 14px;
    transition: background-color 0.2s;
}

.create-task-btn:hover {
    background-color: #388E3C;
}

.filter-container {
  margin-bottom: 20px;
}

.dropdown-filter {
  position: relative;
  width: 250px;
  user-select: none;
}

.dropdown-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 15px;
  background-color: white;
  border: 1px solid #ddd;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s;
}

.dropdown-header:hover {
  background-color: #f5f5f5;
}

.dropdown-arrow {
  font-size: 12px;
  transition: transform 0.2s ease;
}

.dropdown-arrow.open {
  transform: rotate(180deg);
}

.dropdown-menu {
  position: absolute;
  top: 100%;
  left: 0;
  width: 100%;
  margin-top: 5px;
  background-color: white;
  border: 1px solid #ddd;
  border-radius: 4px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
  z-index: 100;
  max-height: 300px;
  overflow-y: auto;
}

.dropdown-item {
  display: flex;
  align-items: center;
  padding: 8px 15px;
  transition: background-color 0.2s;
}

.dropdown-item:hover {
  background-color: #f5f5f5;
}

.dropdown-item input[type="checkbox"] {
  margin-right: 10px;
}

.status-label {
  display: inline-block;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 13px;
  flex-grow: 1;
}

.dropdown-actions {
  display: flex;
  padding: 10px 15px;
  border-top: 1px solid #eee;
  gap: 10px;
}

.select-all-btn {
  background-color: #e0e0e0;
  border: none;
  color: #333;
  padding: 6px 12px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 13px;
  flex: 1;
}

.select-all-btn:hover {
  background-color: #d0d0d0;
}

.clear-filter-btn {
  background-color: #f0f0f0;
  border: none;
  color: #666;
  padding: 6px 12px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 13px;
  flex: 1;
}

.clear-filter-btn:hover {
  background-color: #e0e0e0;
}
</style>
