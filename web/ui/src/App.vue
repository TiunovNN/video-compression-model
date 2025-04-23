<template>
  <h1>Encoding Video!</h1>
  <p><strong>Текущий путь маршрута:</strong> {{ $route.fullPath }}</p>
  <nav>
    <RouterLink to="/">Список задач</RouterLink>
    <RouterLink to="/tasks/create">Загрузить видео</RouterLink>
  </nav>
  <main>
    <RouterView />
  </main>
</template>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
