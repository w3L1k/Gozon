GoZon — микросервисное приложение с асинхронными платежами
Описание проекта

GoZon — учебное микросервисное приложение, демонстрирующее асинхронное взаимодействие сервисов через брокер сообщений RabbitMQ с использованием паттернов Outbox / Inbox.

Проект реализует бизнес-сценарий:

создание заказа,

асинхронную обработку платежа,

обновление статуса заказа на основании результата платежа.

Приложение полностью разворачивается локально с помощью Docker Compose.

Архитектура
Сервисы

Gateway — единая точка входа (API Gateway)

OrdersService — управление заказами

PaymentsService — обработка платежей и баланса

RabbitMQ — брокер сообщений

Frontend (Nginx) — простой UI для проверки бизнес-логики

Взаимодействие

Orders → Payments: очередь orders.payment.request

Payments → Orders: очередь orders.payment.result

Гарантии доставки обеспечиваются через:

Outbox (исходящие события)

Inbox (идемпотентность входящих сообщений)

Retry-механизмы в BackgroundService

Технологии

.NET 8 (C#)

ASP.NET Core Web API

RabbitMQ

SQLite

Docker / Docker Compose

Nginx (frontend)

Swagger (OpenAPI)

Требования для запуска

Docker Desktop (Windows / macOS / Linux)

Docker Compose (входит в Docker Desktop)

Никаких дополнительных зависимостей устанавливать не требуется.

Запуск приложения
1. Клонировать репозиторий
git clone <repo-url>
cd Gozon

2. Запустить все сервисы

Из корня проекта:

docker compose -f .\Docker\docker-compose.yml up -d --build

3. Проверить, что контейнеры запущены
docker ps


Должны быть запущены контейнеры:

gozon-gateway

gozon-orders

gozon-payments

gozon-rabbitmq

gozon-frontend

Доступные URL
Frontend (UI)

Простой интерфейс для проверки бизнес-логики:

http://localhost:3000


Позволяет:

создавать заказы

проверять статус заказа

наблюдать асинхронное изменение статуса

API Gateway
http://localhost:8080


Все запросы к OrdersService проксируются через Gateway.

OrdersService (Swagger)
http://localhost:8081/swagger


Основные эндпоинты:

POST /orders — создать заказ

GET /orders — список заказов

GET /orders/{id} — получить заказ по id

PaymentsService (Swagger)
http://localhost:8082/swagger


Используется для обработки платежей и баланса (основная логика — асинхронная).

RabbitMQ Management UI
http://localhost:15672


Логин / пароль:

guest / guest


Очереди:

orders.payment.request

orders.payment.result

Проверка бизнес-логики
Успешный платёж

Создать заказ с небольшой суммой (например, 150)

Статус заказа через несколько секунд станет Finished / Paid

Неуспешный платёж

Создать заказ с суммой, превышающей баланс (например, 999999)

Статус заказа станет Cancelled / Failed

Это подтверждает корректную работу логики баланса и асинхронного взаимодействия сервисов.

Надёжность и отказоустойчивость

Все RabbitMQ consumers используют retry-механизмы

Сервисы не падают при временной недоступности брокера

Идемпотентность сообщений реализована через Inbox

Доставка событий гарантирована через Outbox
