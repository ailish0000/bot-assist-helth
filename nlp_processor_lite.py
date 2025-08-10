"""
🧠 Облегченный NLP Процессор для бота-нутрициолога
Версия без тяжелых ML моделей для быстрой загрузки
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class QuestionIntent:
    """Структура для хранения анализа намерения вопроса"""
    intent_type: str  # medical, organizational, recipe, product, greeting, etc.
    confidence: float  # уверенность в классификации 0-1
    keywords: List[str]  # ключевые слова
    expanded_query: str  # расширенный запрос с синонимами
    context_hints: List[str]  # подсказки для поиска
    response_tone: str  # тон ответа: formal, friendly, medical, etc.

class NLPProcessorLite:
    """
    Облегченный класс для NLP-обработки студенческих вопросов
    Без использования тяжелых ML моделей
    """
    
    def __init__(self):
        """Инициализация облегченного NLP процессора"""
        self._init_knowledge_base()
        logger.info("✅ Облегченный NLP Процессор инициализирован")
    
    def _init_knowledge_base(self):
        """Инициализация базы знаний для расширения запросов"""
        
        # Медицинские синонимы и связанные термины
        self.medical_synonyms = {
            # Боли и симптомы
            "боль": ["болит", "болезненно", "неприятные ощущения", "дискомфорт", "ноет", "колет", "тянет"],
            "позвоночник": ["спина", "позвоночный столб", "хребет", "поясница", "грудной отдел", "шейный отдел"],
            "суставы": ["сочленения", "колени", "локти", "плечи", "тазобедренные", "голеностоп"],
            "головная боль": ["мигрень", "цефалгия", "головные боли", "боли в голове", "тяжесть в голове"],
            "тошнота": ["подташнивает", "рвотные позывы", "тошнит", "мутит", "дурнота"],
            
            # Методы лечения
            "тейпирование": ["кинезиотейпинг", "тейпы", "пластыри", "кинезио тейп", "терапевтические пластыри"],
            "массаж": ["растирание", "разминание", "мануальная терапия", "лечебный массаж"],
            "упражнения": ["гимнастика", "ЛФК", "лечебная физкультура", "зарядка", "тренировки"],
            
            # Продукты и питание
            "диета": ["рацион", "питание", "меню", "режим питания"],
            "витамины": ["микроэлементы", "добавки", "БАДы", "нутриенты", "минералы"],
            "белки": ["протеины", "аминокислоты", "белковая пища"],
            "углеводы": ["сахара", "крахмал", "глюкоза", "энергия"],
            "жиры": ["липиды", "масла", "омега", "жирные кислоты"]
        }
        
        # Организационные термины
        self.organizational_synonyms = {
            "куратор": ["преподаватель", "наставник", "тьютор", "учитель", "инструктор"],
            "обучение": ["курс", "программа", "занятия", "уроки", "лекции"],
            "экзамен": ["зачет", "тест", "проверка знаний", "аттестация", "контроль"],
            "сертификат": ["диплом", "удостоверение", "документ об образовании"],
            "расписание": ["график", "время занятий", "план", "календарь"],
            "задание": ["домашнее задание", "упражнение", "практика", "работа"]
        }
        
        # Кулинарные термины
        self.cooking_synonyms = {
            "рецепт": ["способ приготовления", "инструкция", "как готовить", "как сделать"],
            "ингредиенты": ["продукты", "составляющие", "компоненты"],
            "приготовление": ["готовка", "варка", "жарка", "тушение", "запекание"],
            "специи": ["приправы", "пряности", "травы", "зелень"],
            "соус": ["заправка", "подлива", "дрессинг", "майонез"]
        }
        
        # Типы интентов с ключевыми словами
        self.intent_patterns = {
            "medical_question": {
                "keywords": ["боль", "болит", "симптом", "лечение", "тейп", "массаж", "помогает", "можно ли", "противопоказания"],
                "patterns": [r"болит", r"помогает ли", r"можно ли", r"лечить", r"тейп", r"противопоказан"],
                "response_tone": "medical_professional"
            },
            "organizational_question": {
                "keywords": ["куратор", "экзамен", "сертификат", "обучение", "расписание", "когда", "где", "как получить"],
                "patterns": [r"когда.*экзамен", r"как.*связаться", r"где.*расписание", r"сколько.*длится"],
                "response_tone": "helpful_administrative"
            },
            "recipe_question": {
                "keywords": ["рецепт", "как готовить", "ингредиенты", "приготовить", "блюдо", "меню"],
                "patterns": [r"рецепт", r"как.*готовить", r"приготовить", r"ингредиенты"],
                "response_tone": "friendly_cooking"
            },
            "product_question": {
                "keywords": ["продукт", "полезно", "вредно", "калории", "состав", "можно есть"],
                "patterns": [r"полезн", r"вредн", r"калори", r"можно.*есть", r"состав"],
                "response_tone": "nutritional_expert"
            },
            "greeting": {
                "keywords": ["привет", "здравствуй", "добро", "спасибо", "благодарю"],
                "patterns": [r"привет", r"здравствуй", r"добр", r"спасибо", r"благодар"],
                "response_tone": "friendly_greeting"
            }
        }
        
    def analyze_question(self, question: str) -> QuestionIntent:
        """
        Полный анализ вопроса студента
        
        Args:
            question: Текст вопроса
            
        Returns:
            QuestionIntent: Структурированный анализ вопроса
        """
        question_lower = question.lower().strip()
        
        # 1. Определяем тип интента
        intent_type, confidence = self._classify_intent(question_lower)
        
        # 2. Извлекаем ключевые слова
        keywords = self._extract_keywords(question_lower, intent_type)
        
        # 3. Расширяем запрос синонимами
        expanded_query = self._expand_query_with_synonyms(question, keywords, intent_type)
        
        # 4. Генерируем подсказки для поиска
        context_hints = self._generate_context_hints(keywords, intent_type)
        
        # 5. Определяем тон ответа
        response_tone = self.intent_patterns[intent_type]["response_tone"]
        
        result = QuestionIntent(
            intent_type=intent_type,
            confidence=confidence,
            keywords=keywords,
            expanded_query=expanded_query,
            context_hints=context_hints,
            response_tone=response_tone
        )
        
        logger.info(f"🧠 Анализ вопроса: {intent_type} (уверенность: {confidence:.2f})")
        logger.info(f"🔑 Ключевые слова: {keywords}")
        logger.info(f"📝 Расширенный запрос: {expanded_query[:100]}...")
        
        return result
    
    def _classify_intent(self, question: str) -> Tuple[str, float]:
        """Классификация типа вопроса"""
        max_score = 0
        best_intent = "general_question"
        
        for intent, config in self.intent_patterns.items():
            score = 0
            
            # Проверяем ключевые слова
            for keyword in config["keywords"]:
                if keyword in question:
                    score += 2
            
            # Проверяем паттерны
            for pattern in config["patterns"]:
                if re.search(pattern, question, re.IGNORECASE):
                    score += 3
            
            # Нормализуем score
            confidence = min(score / 10, 1.0)
            
            if confidence > max_score:
                max_score = confidence
                best_intent = intent
        
        return best_intent, max_score
    
    def _extract_keywords(self, question: str, intent_type: str) -> List[str]:
        """Извлечение ключевых слов из вопроса"""
        keywords = []
        
        # Базовые ключевые слова по типу интента
        if intent_type in self.intent_patterns:
            intent_keywords = self.intent_patterns[intent_type]["keywords"]
            for keyword in intent_keywords:
                if keyword in question:
                    keywords.append(keyword)
        
        # Медицинские термины
        for term, synonyms in self.medical_synonyms.items():
            if term in question or any(syn in question for syn in synonyms):
                keywords.append(term)
        
        # Организационные термины
        for term, synonyms in self.organizational_synonyms.items():
            if term in question or any(syn in question for syn in synonyms):
                keywords.append(term)
        
        # Кулинарные термины
        for term, synonyms in self.cooking_synonyms.items():
            if term in question or any(syn in question for syn in synonyms):
                keywords.append(term)
        
        return list(set(keywords))  # убираем дубликаты
    
    def _expand_query_with_synonyms(self, original_query: str, keywords: List[str], intent_type: str) -> str:
        """Расширение запроса синонимами для улучшения поиска"""
        expanded_parts = [original_query]
        
        # Добавляем синонимы найденных ключевых слов
        for keyword in keywords:
            synonyms = []
            
            # Медицинские синонимы
            if keyword in self.medical_synonyms:
                synonyms.extend(self.medical_synonyms[keyword][:3])  # берем первые 3
            
            # Организационные синонимы
            if keyword in self.organizational_synonyms:
                synonyms.extend(self.organizational_synonyms[keyword][:3])
            
            # Кулинарные синонимы
            if keyword in self.cooking_synonyms:
                synonyms.extend(self.cooking_synonyms[keyword][:3])
            
            if synonyms:
                expanded_parts.append(" ".join(synonyms))
        
        # Добавляем контекстные термины по типу интента
        context_terms = {
            "medical_question": ["лечение", "терапия", "показания", "эффективность"],
            "organizational_question": ["информация", "правила", "процедура", "требования"],
            "recipe_question": ["приготовление", "кулинария", "способ", "метод"],
            "product_question": ["свойства", "польза", "применение", "рекомендации"]
        }
        
        if intent_type in context_terms:
            expanded_parts.append(" ".join(context_terms[intent_type]))
        
        return " ".join(expanded_parts)
    
    def _generate_context_hints(self, keywords: List[str], intent_type: str) -> List[str]:
        """Генерация подсказок для более точного поиска контекста"""
        hints = []
        
        # Добавляем подсказки на основе ключевых слов
        for keyword in keywords:
            if keyword in ["боль", "болит"]:
                hints.extend(["болевой синдром", "обезболивание", "анальгезия"])
            elif keyword in ["тейпирование", "тейп"]:
                hints.extend(["кинезиотейпинг", "терапевтическое тейпирование", "реабилитация"])
            elif keyword in ["куратор", "преподаватель"]:
                hints.extend(["связь с куратором", "контакты", "обращение к преподавателю"])
            elif keyword in ["рецепт", "приготовление"]:
                hints.extend(["кулинарный рецепт", "способ приготовления", "инструкция"])
        
        # Добавляем подсказки по типу интента
        intent_hints = {
            "medical_question": ["медицинские показания", "терапевтическое применение", "клинические рекомендации"],
            "organizational_question": ["учебный процесс", "административные вопросы", "регламент"],
            "recipe_question": ["кулинарные рецепты", "приготовление пищи", "пищевые технологии"],
            "product_question": ["пищевые продукты", "нутриентный состав", "диетические рекомендации"]
        }
        
        if intent_type in intent_hints:
            hints.extend(intent_hints[intent_type])
        
        return list(set(hints))  # убираем дубликаты
    
    def enhance_search_query(self, original_query: str) -> Dict[str, str]:
        """
        Создает несколько вариантов поискового запроса для более точного поиска
        
        Returns:
            Dict с различными вариантами запроса
        """
        intent = self.analyze_question(original_query)
        
        return {
            "original": original_query,
            "expanded": intent.expanded_query,
            "keywords_only": " ".join(intent.keywords),
            "context_hints": " ".join(intent.context_hints),
            "combined": f"{original_query} {' '.join(intent.keywords)} {' '.join(intent.context_hints[:3])}"
        }
    
    def suggest_related_questions(self, question: str, found_docs: List[str]) -> List[str]:
        """
        Предлагает связанные вопросы на основе найденного контента
        """
        intent = self.analyze_question(question)
        suggestions = []
        
        # Базовые предложения по типу интента
        if intent.intent_type == "medical_question":
            suggestions = [
                "Какие есть противопоказания к этому методу?",
                "Как долго применять это лечение?",
                "Есть ли побочные эффекты?"
            ]
        elif intent.intent_type == "organizational_question":
            suggestions = [
                "Где найти подробную информацию об этом?",
                "К кому обратиться за помощью?",
                "Какие документы нужны?"
            ]
        elif intent.intent_type == "recipe_question":
            suggestions = [
                "Какие есть альтернативные ингредиенты?",
                "Сколько времени займет приготовление?",
                "Можно ли изменить рецепт?"
            ]
        
        return suggestions[:3]  # возвращаем не более 3 предложений

# Для обратной совместимости
NLPProcessor = NLPProcessorLite
