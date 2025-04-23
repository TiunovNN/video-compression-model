<template>
  <img width="128" src="./assets/logo.png">
  <h1>Encoding Video!</h1>
  <nav class="tabs-navigation">
    <RouterLink to="/" class="tab-link">Список задач</RouterLink>
    <RouterLink to="/create" class="tab-link">Загрузить видео</RouterLink>
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

/* Добавьте эти стили к существующим для создания вкладок навигации */

/* Контейнер для навигационных вкладок */
.tabs-navigation {
  display: flex;
  border-bottom: 1px solid #ddd;
  margin-bottom: 20px;
  padding: 0;
  background-color: #f8f8f8;
  border-radius: 6px 6px 0 0;
  overflow: hidden;
}

/* Базовый стиль для всех вкладок (ссылок) */
.tab-link {
  display: inline-block;
  padding: 12px 18px;
  text-decoration: none;
  color: #555;
  font-weight: 500;
  position: relative;
  transition: all 0.2s ease;
  border-right: 1px solid #ddd;
  text-align: center;
  flex: 1;
  font-size: 15px;
}

.tab-link:hover {
  background-color: #efefef;
  color: #333;
}

/* Активная вкладка */
.router-link-active,
.router-link-exact-active {
  background-color: #ffffff;
  color: #2196F3;
  font-weight: 600;
  border-bottom: 3px solid #2196F3;
  margin-bottom: -1px;
}

/* Эффект нажатия */
.tab-link:active {
  background-color: #e0e0e0;
}

/* Иконки для вкладок (если нужны) */
.tab-icon {
  margin-right: 8px;
  font-size: 16px;
  vertical-align: middle;
}

/* Счетчик для вкладок (опционально) */
.tab-counter {
  display: inline-block;
  background-color: #eeeeee;
  color: #666;
  border-radius: 10px;
  padding: 2px 8px;
  font-size: 12px;
  margin-left: 5px;
}

/* Для активной вкладки счетчик будет выделен */
.router-link-active .tab-counter,
.router-link-exact-active .tab-counter {
  background-color: #bbdefb;
  color: #1976D2;
}

/* Адаптивный дизайн для мобильных устройств */
@media (max-width: 768px) {
  .tabs-navigation {
    flex-direction: column;
    border-radius: 6px;
  }

  .tab-link {
    border-right: none;
    border-bottom: 1px solid #ddd;
  }

  .router-link-active,
  .router-link-exact-active {
    border-bottom: 1px solid #ddd;
    border-left: 3px solid #2196F3;
    border-bottom: none;
  }
}
</style>
