import { createApp } from 'vue'
import App from './App.vue'
import router from './routes.js'

createApp(App)
    .provide('apiURI', 'http://localhost:8000')
    .use(router)
    .mount('#app')
