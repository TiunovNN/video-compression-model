<template>
    <div class="container">
        <h1>Информация о видео</h1>

        <div v-if="loading" class="loading">
            Загрузка данных...
        </div>

        <div v-else>
            <div class="status-container">
                <h2>Статус задачи: <span :class="statusClass">{{ StatusLabel }}</span></h2>
            </div>
            <div v-if="taskStatus === 'completed'" class="file-info">
                <div class="info-row">
                    <span>Исходный размер:</span>
                    <span>{{ formattedSourceSize }}</span>
                </div>
                <div class="info-row" v-if="fileInfo?.output_size">
                    <span>Размер после обработки:</span>
                    <span>{{ formattedOutputSize }}</span>
                </div>
                <div class="info-row" v-if="compressionRatio">
                    <span>Степень сжатия:</span>
                    <span>{{ compressionRatio }}x</span>
                </div>
            </div>

            <div v-if="videoUrl" class="video-container">
                <video controls width="100%">
                    <source :src="videoUrl" type="video/mp4">
                    Ваш браузер не поддерживает воспроизведение видео.
                </video>
            </div>
            <div v-if="errorMessage" class="error-container">
                <div class="error-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none"
                        stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="12" cy="12" r="10"></circle>
                        <line x1="12" y1="8" x2="12" y2="12"></line>
                        <line x1="12" y1="16" x2="12.01" y2="16"></line>
                    </svg>
                </div>
                <div class="error-message">
                    {{ errorMessage }}
                </div>
            </div>

            <button v-if="videoUrl" @click="downloadVideo" class="download-button">
                Скачать видео
            </button>
        </div>
    </div>
</template>

<script>
export default {
    props: {
        taskId: {
            type: String,
            required: true
        }
    },
    data() {
        return {
            taskStatus: 'Ожидание...',
            videoUrl: null,
            fileInfo: null,
            loading: true,
            statusInterval: null,
            errorMessage: null
        };
    },
    inject: ['apiURI'],
    computed: {
        statusClass() {
            if (this.taskStatus === 'completed') return 'status-completed';
            if (this.taskStatus === 'processing') return 'status-processing';
            if (this.taskStatus === 'error') return 'status-error';
            return '';
        },
        StatusLabel() {
            const statusMap = {
                pending: 'Ожидает',
                processing: 'Обрабатывается',
                completed: 'Завершено',
                failed: 'Ошибка'
            };

            return statusMap[this.taskStatus] || this.taskStatus;
        },
        formattedSourceSize() {
            return this.formatFileSize(this.fileInfo?.source_size);
        },

        formattedOutputSize() {
            return this.formatFileSize(this.fileInfo?.output_size);
        },

        compressionRatio() {
            if (!this.fileInfo?.source_size || !this.fileInfo?.output_size) return null;
            const ratio = this.fileInfo.source_size / this.fileInfo.output_size;
            return ratio.toFixed(2);
        },

    },
    mounted() {
        this.initData();
    },
    beforeUnmount() {
        // Очистка интервала при удалении компонента
        if (this.statusInterval) {
            clearInterval(this.statusInterval);
        }
    },
    methods: {
        initData() {
            this.updateTaskStatus();

            // Обновляем статус задачи каждые 5 секунд
            this.statusInterval = setInterval(() => {
                this.updateTaskStatus();
            }, 5000);
        },
        async updateTaskStatus() {
            try {
                const response = await fetch(`${this.apiURI}/tasks/${this.taskId}`);
                if (!response.ok) throw new Error('Не удалось получить статус задачи');

                const data = await response.json();
                this.taskStatus = data.status || data; // В зависимости от формата ответа
                // Сохраняем информацию о файле
                this.fileInfo = data;

                // Если задача завершена, получаем URL видео и информацию о файле
                if (this.taskStatus === 'completed') {
                    this.videoUrl = data.download_url;

                    // Останавливаем интервал, если задача завершена
                    clearInterval(this.statusInterval);
                }
                if (this.taskStatus === 'failed') {
                    this.errorMessage = data.error_message;
                    clearInterval(this.statusInterval);
                }

                this.loading = false;
            } catch (error) {
                console.error('Ошибка при получении статуса задачи:', error);
            }
        },
        downloadVideo() {
            if (!this.videoUrl) return;

            const link = document.createElement('a');
            link.href = this.videoUrl;
            link.setAttribute('download', `video-${this.taskId}.mp4`);
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        },
        formatFileSize(bytes) {
            if (!bytes) return 'N/A';

            const units = ['B', 'KB', 'MB', 'GB'];
            let size = bytes;
            let unitIndex = 0;

            while (size >= 1024 && unitIndex < units.length - 1) {
                size /= 1024;
                unitIndex++;
            }

            return `${size.toFixed(2)} ${units[unitIndex]}`;
        },
    }
};
</script>

<style scoped>
.container {
    max-width: 800px;
    margin: 0 auto;
    padding: 20px;
    font-family: Arial, sans-serif;
}

.loading {
    text-align: center;
    font-size: 18px;
    margin: 30px 0;
}

.status-container {
    margin-bottom: 20px;
}

.status-completed {
    color: green;
}

.status-processing {
    color: orange;
}

.status-error {
    color: red;
}

.video-container {
    margin-bottom: 20px;
    background-color: #f5f5f5;
    padding: 10px;
    border-radius: 4px;
}

.info-container {
    background-color: #f9f9f9;
    padding: 15px;
    border-radius: 4px;
    margin-bottom: 20px;
}

.download-button {
    background-color: #4CAF50;
    border: none;
    color: white;
    padding: 12px 24px;
    text-align: center;
    text-decoration: none;
    display: inline-block;
    font-size: 16px;
    margin: 4px 2px;
    cursor: pointer;
    border-radius: 4px;
    transition: background-color 0.3s;
}

.download-button:hover {
    background-color: #45a049;
}

.error-container {
    display: flex;
    align-items: flex-start;
    background-color: #FFEBEE;
    border: 1px solid #FFCDD2;
    border-radius: 8px;
    padding: 12px 16px;
    margin: 10px 0;
    color: #D32F2F;
    text-align: left;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

.error-icon {
    flex-shrink: 0;
    margin-right: 12px;
    margin-top: 2px;
}

.error-message {
    font-size: 14px;
    line-height: 1.5;
    word-wrap: break-word;
}
.file-info {
    background-color: #f5f5f5;
    padding: 15px;
    border-radius: 4px;
    margin: 15px 0;
}

.info-row {
    display: flex;
    justify-content: space-between;
    margin-bottom: 8px;
}

.info-row:last-child {
    margin-bottom: 0;
}

.info-row span:first-child {
    font-weight: bold;
}

/* Анимация появления ошибки */
@keyframes errorFadeIn {
    from {
        opacity: 0;
        transform: translateY(-10px);
    }

    to {
        opacity: 1;
        transform: translateY(0);
    }
}

.error-container {
    animation: errorFadeIn 0.3s ease-out;
}
</style>
