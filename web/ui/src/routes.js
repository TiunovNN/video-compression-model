import { createWebHistory, createRouter } from 'vue-router'
// import { publicPath } from '../vue.config.js'
import UploadPage from './components/UploadPage.vue'
import TaskListPage from './components/TaskListPage.vue'
import PreviewPage from './components/PreviewPage.vue';

const routes = [
    { path: '/', component: TaskListPage },
    { path: '/view/:taskId', component: PreviewPage, props: true },
    { path: '/create', component: UploadPage }
]

const router = createRouter({
    history: createWebHistory('/app'),
    routes
})

export default router
