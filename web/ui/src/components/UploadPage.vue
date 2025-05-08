<template>
    <div class="upload-container">
        <h1>Загрузка видео</h1>

        <div class="upload-area" :class="{ 'drag-over': isDragging }" @dragover.prevent="onDragOver"
            @dragleave.prevent="onDragLeave" @drop.prevent="onDrop">

            <div v-if="!file && !isUploading" class="upload-prompt">
                <i class="upload-icon">&#8682;</i>
                <p>Перетащите видеофайл сюда или</p>
                <label for="file-input" class="file-input-label">
                    Выберите файл
                    <input type="file" id="file-input" accept="video/*,.mkv" @change="onFileSelected" class="file-input" />
                </label>
            </div>

            <div v-if="file && !isUploading" class="file-info">
                <h3>Выбранный файл:</h3>
                <p><strong>Название:</strong> {{ file.name }}</p>
                <p><strong>Размер:</strong> {{ formatFileSize(file.size) }}</p>
                <p v-if="file.type"><strong>Тип:</strong> {{ file.type }}</p>

                <div class="action-buttons">
                    <button @click="uploadVideo" class="upload-button">Загрузить</button>
                    <button @click="resetFile" class="cancel-button">Отмена</button>
                </div>
            </div>

            <div v-if="isUploading" class="upload-progress">
                <div class="progress-bar">
                    <div class="progress-fill" :style="{ width: `${uploadProgress}%` }"></div>
                </div>
                <p>{{ uploadProgress }}% загружено</p>
                <button @click="cancelUpload" class="cancel-button">Отменить загрузку</button>
            </div>
        </div>

        <div v-if="uploadError" class="error-message">
            <p>{{ uploadError }}</p>
        </div>

        <div v-if="uploadSuccess" class="success-message">
            <p>{{ uploadSuccess }}</p>
            <p v-if="taskId">ID задачи: <strong>{{ taskId }}</strong></p>
            <button v-if="taskId" @click="goToTaskPage" class="view-task-button">
                Перейти к задаче
            </button>
        </div>

        <div class="upload-limits">
            <h3>Ограничения загрузки:</h3>
            <ul>
                <li>Максимальный размер файла: 15GB</li>
                <li>Поддерживаемые форматы: MP4, AVI, MOV, MKV</li>
            </ul>
        </div>
    </div>
</template>

<script>
export default {
    data() {
        return {
            file: null,
            isDragging: false,
            isUploading: false,
            uploadProgress: 0,
            uploadError: null,
            uploadSuccess: null,
            taskId: null,
            abortController: null
        };
    },
    inject: ['apiURI'],
    methods: {
        onDragOver() {
            this.isDragging = true;
        },
        onDragLeave() {
            this.isDragging = false;
        },
        onDrop(event) {
            this.isDragging = false;
            if (event.dataTransfer.files.length) {
                const file = event.dataTransfer.files[0];
                if (this.validateFile(file)) {
                    this.file = file;
                }
            }
        },
        onFileSelected(event) {
            const file = event.target.files[0];
            if (file && this.validateFile(file)) {
                this.file = file;
            }
            // Сбрасываем input, чтобы можно было выбрать тот же файл повторно
            event.target.value = '';
        },
        validateFile(file) {
            // Проверка размера (15GB)
            const maxSize = 15 * 1024 * 1024 * 1024;
            if (file.size > maxSize) {
                this.uploadError = 'Файл слишком большой. Максимальный размер 15GB.';
                return false;
            }

            // Проверка типа файла
            const validTypes = ['video/mp4', 'video/avi', 'video/quicktime', 'video/x-matroska'];
            if (!file.type || !validTypes.includes(file.type)) {
                // Проверка по расширению, если тип не определен
                const extension = file.name.split('.').pop().toLowerCase();
                const validExtensions = ['mp4', 'avi', 'mov', 'mkv'];
                if (!validExtensions.includes(extension)) {
                    this.uploadError = 'Неподдерживаемый тип файла. Пожалуйста, загрузите видео в формате MP4, AVI, MOV или MKV.';
                    return false;
                }
            }

            this.uploadError = null;
            return true;
        },
        resetFile() {
            this.file = null;
            this.uploadError = null;
            this.uploadSuccess = null;
            this.taskId = null;
        },
        async uploadVideo() {
            if (!this.file) return;

            this.isUploading = true;
            this.uploadProgress = 0;
            this.uploadError = null;
            this.uploadSuccess = null;
            this.taskId = null;

            const formData = new FormData();
            formData.append('file', this.file);

            this.abortController = new AbortController();
            const signal = this.abortController.signal;

            try {
                const xhr = new XMLHttpRequest();
                xhr.open('POST', `${this.apiURI}/tasks`, true);

                // Отслеживаем прогресс загрузки
                xhr.upload.addEventListener('progress', (event) => {
                    if (event.lengthComputable) {
                        this.uploadProgress = Math.round((event.loaded / event.total) * 100);
                    }
                });

                // Создаем Promise для XHR запроса
                const uploadPromise = new Promise((resolve, reject) => {
                    xhr.onload = () => {
                        if (xhr.status >= 200 && xhr.status < 300) {
                            resolve(xhr.response);
                        } else {
                            reject(new Error(`HTTP ошибка: ${xhr.status}`));
                        }
                    };
                    xhr.onerror = () => reject(new Error('Ошибка сети'));
                    xhr.ontimeout = () => reject(new Error('Таймаут'));
                });

                // Обработка отмены загрузки
                signal.addEventListener('abort', () => {
                    xhr.abort();
                });

                // Начинаем загрузку
                xhr.responseType = 'json';
                xhr.send(formData);

                // Ожидаем завершения загрузки
                const response = await uploadPromise;

                // Обрабатываем успешный ответ
                this.taskId = response.taskId || response.id || null;
                this.uploadSuccess = 'Видео успешно загружено!';
                this.file = null;
            } catch (error) {
                // Если ошибка не из-за отмены загрузки
                if (!signal.aborted) {
                    this.uploadError = `Ошибка загрузки: ${error.message}`;
                    console.error('Ошибка загрузки:', error);
                }
            } finally {
                this.isUploading = false;
                this.abortController = null;
            }
        },
        cancelUpload() {
            if (this.abortController) {
                this.abortController.abort();
                this.isUploading = false;
                this.uploadProgress = 0;
            }
        },
        formatFileSize(bytes) {
            if (bytes === 0) return '0 Байт';

            const sizes = ['Байт', 'КБ', 'МБ', 'ГБ'];
            const i = Math.floor(Math.log(bytes) / Math.log(1024));
            return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
        },
        goToTaskPage() {
            if (this.taskId) {
                // Перенаправление на страницу задачи
                this.$router.push(`/view/${this.taskId}`);
            }
        }
    }
};
</script>

<style scoped>
.upload-container {
    max-width: 800px;
    margin: 0 auto;
    padding: 20px;
    font-family: Arial, sans-serif;
}

h1 {
    text-align: center;
    color: #333;
    margin-bottom: 30px;
}

.upload-area {
    border: 2px dashed #ccc;
    border-radius: 8px;
    padding: 40px;
    text-align: center;
    margin-bottom: 20px;
    transition: all 0.3s ease;
    background-color: #f9f9f9;
}

.drag-over {
    border-color: #4CAF50;
    background-color: rgba(76, 175, 80, 0.1);
}

.upload-icon {
    font-size: 48px;
    display: block;
    margin-bottom: 15px;
    color: #666;
}

.upload-prompt p {
    margin-bottom: 20px;
    color: #666;
    font-size: 16px;
}

.file-input {
    display: none;
}

.file-input-label {
    background-color: #4CAF50;
    color: white;
    padding: 10px 20px;
    border-radius: 4px;
    cursor: pointer;
    display: inline-block;
    font-size: 14px;
    transition: background-color 0.3s;
}

.file-input-label:hover {
    background-color: #45a049;
}

.file-info {
    text-align: left;
    padding: 15px;
    background-color: #fff;
    border-radius: 4px;
    box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
}

.file-info h3 {
    margin-top: 0;
    color: #333;
}

.action-buttons {
    margin-top: 20px;
    display: flex;
    justify-content: center;
    gap: 10px;
}

.upload-button,
.view-task-button {
    background-color: #4CAF50;
    border: none;
    color: white;
    padding: 10px 20px;
    text-align: center;
    text-decoration: none;
    font-size: 14px;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 0.3s;
}

.upload-button:hover,
.view-task-button:hover {
    background-color: #45a049;
}

.cancel-button {
    background-color: #f44336;
    border: none;
    color: white;
    padding: 10px 20px;
    text-align: center;
    text-decoration: none;
    font-size: 14px;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 0.3s;
}

.cancel-button:hover {
    background-color: #d32f2f;
}

.upload-progress {
    padding: 15px;
}

.progress-bar {
    height: 20px;
    background-color: #f0f0f0;
    border-radius: 10px;
    margin-bottom: 10px;
    overflow: hidden;
}

.progress-fill {
    height: 100%;
    background-color: #4CAF50;
    transition: width 0.3s ease;
}

.error-message {
    color: #f44336;
    background-color: #ffebee;
    padding: 15px;
    border-radius: 4px;
    margin-bottom: 20px;
    border-left: 4px solid #f44336;
}

.success-message {
    color: #4CAF50;
    background-color: #e8f5e9;
    padding: 15px;
    border-radius: 4px;
    margin-bottom: 20px;
    border-left: 4px solid #4CAF50;
    text-align: center;
}

.upload-limits {
    margin-top: 30px;
    padding: 15px;
    background-color: #f5f5f5;
    border-radius: 4px;
}

.upload-limits h3 {
    margin-top: 0;
    color: #333;
    font-size: 16px;
}

.upload-limits ul {
    padding-left: 20px;
    color: #666;
}
</style>
