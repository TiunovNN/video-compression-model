import { createMemoryHistory, createRouter } from 'vue-router'
import UploadPage from './components/UploadPage.vue'
import TaskListPage from './components/TaskListPage.vue'
import PreviewPage from './components/PreviewPage.vue';

const routes = [
    { path: '/', component: TaskListPage },
    { path: '/tasks/:taskId', component: PreviewPage, props: true },
    { path: '/tasks/create', component: UploadPage }
]

const router = createRouter({
    history: createMemoryHistory(),
    routes
})

export default router
