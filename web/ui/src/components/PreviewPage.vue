<template>
    <div class="container">
        <h1>Информация о видео</h1>

        <div v-if="loading" class="loading">
            Загрузка данных...
        </div>

        <div v-else>
            <div class="status-container">
                <h2>Статус задачи: <span :class="statusClass">{{ taskStatus }}</span></h2>
            </div>

            <div v-if="videoUrl" class="video-container">
                <video controls width="100%">
                    <source :src="videoUrl" type="video/mp4">
                    Ваш браузер не поддерживает воспроизведение видео.
                </video>
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
            statusInterval: null
        };
    },
    inject: ['apiURI'],
    computed: {
        statusClass() {
            if (this.taskStatus === 'completed') return 'status-completed';
            if (this.taskStatus === 'processing') return 'status-processing';
            if (this.taskStatus === 'error') return 'status-error';
            return '';
        }
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

                // Если задача завершена, получаем URL видео и информацию о файле
                if (this.taskStatus === 'completed') {
                    this.videoUrl = data.download_url

                    // Останавливаем интервал, если задача завершена
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
</style>
