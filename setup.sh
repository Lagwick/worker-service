#!/bin/bash

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Проверка аргументов
if [ -z "$1" ] || [ -z "$2" ]; then
    echo -e "${RED}Ошибка: Не указаны обязательные параметры${NC}"
    echo ""
    echo "Использование: ./setup.sh NEW_PROJECT_NAME GITHUB_USERNAME"
    echo "Пример: ./setup.sh my-awesome-service MoM-Repo"
    echo ""
    echo "Параметры:"
    echo "  NEW_PROJECT_NAME  - Имя нового проекта (например: order-service)"
    echo "  GITHUB_USERNAME   - Ваш GitHub username или организация"
    exit 1
fi

NEW_NAME="$1"
GITHUB_USER="$2"
OLD_NAME="mom-boilerplate-v2"
OLD_MODULE="github.com/MoM-Repo/mom-boilerplate-v2"
NEW_MODULE="github.com/$GITHUB_USER/$NEW_NAME"

echo -e "${YELLOW}🚀 Переименование проекта...${NC}"
echo "   Имя: $OLD_NAME → $NEW_NAME"
echo "   Модуль: $OLD_MODULE → $NEW_MODULE"
echo ""

# Функция для замены в файлах (кросс-платформенная)
replace_in_file() {
    local file="$1"
    local old="$2"
    local new="$3"
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s|$old|$new|g" "$file"
    else
        sed -i "s|$old|$new|g" "$file"
    fi
}

# 1. Замена module path в Go файлах (импорты)
echo -e "${YELLOW}📦 Обновление импортов в Go файлах...${NC}"
while IFS= read -r -d '' file; do
    replace_in_file "$file" "$OLD_MODULE" "$NEW_MODULE"
done < <(find . -type f -name "*.go" -print0 2>/dev/null)

# 2. Обновление go.mod
echo -e "${YELLOW}📝 Обновление go.mod...${NC}"
replace_in_file "go.mod" "$OLD_MODULE" "$NEW_MODULE"
go mod edit -module "$NEW_MODULE"

# 3. Замена имени проекта в конфигурационных файлах
echo -e "${YELLOW}⚙️  Обновление конфигурационных файлов...${NC}"
while IFS= read -r -d '' file; do
    replace_in_file "$file" "$OLD_NAME" "$NEW_NAME"
done < <(find . -type f \( -name "*.yaml" -o -name "*.yml" -o -name "Makefile" -o -name "Dockerfile" -o -name "*.md" -o -name ".env*" \) -print0 2>/dev/null)

# 4. Обновление AppName в constant.go
echo -e "${YELLOW}🏷️  Обновление AppName...${NC}"
if [ -f "internal/pkg/constant/constant.go" ]; then
    replace_in_file "internal/pkg/constant/constant.go" "AppName = \"$OLD_NAME\"" "AppName = \"$NEW_NAME\""
fi

# 5. Синхронизация зависимостей
echo -e "${YELLOW}🔄 Синхронизация зависимостей...${NC}"
go mod tidy

echo ""
echo -e "${GREEN}✅ Проект успешно переименован!${NC}"
echo ""
echo -e "${YELLOW}📋 Следующие шаги:${NC}"
echo "   1. Удалите старый .git и инициализируйте новый репозиторий:"
echo "      rm -rf .git && git init"
echo ""
echo "   2. Создайте первый коммит:"
echo "      git add . && git commit -m 'Initial commit: $NEW_NAME'"
echo ""
echo "   3. Добавьте remote и запушьте:"
echo "      git remote add origin git@github.com:$GITHUB_USER/$NEW_NAME.git"
echo "      git push -u origin main"
echo ""
echo "   4. Отредактируйте описание сервиса (Usage) в main.go"
echo ""
echo "   5. Запустите приложение:"
echo "      make run     # Запустить сервер"
