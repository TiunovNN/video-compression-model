import { createApp } from 'vue'
import App from './App.vue'
import router from './routes.js'

createApp(App)
    .provide('apiURI', null)
    .use(router)
    .mount('#app')
