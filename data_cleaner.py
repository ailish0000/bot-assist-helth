"""
🧹 Модуль автоматической очистки данных для бота-нутрициолога
Обеспечивает качественную подготовку данных для обучения и точных эмбеддингов
"""

import re
import logging
from typing import List, Dict, Tuple, Set
from collections import Counter
import unicodedata

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Комплексная система очистки текстовых данных из PDF документов
    """
    
    def __init__(self):
        """Инициализация очистителя данных"""
        self._init_cleaning_rules()
        logger.info("✅ Модуль очистки данных инициализирован")
    
    def _init_cleaning_rules(self):
        """Инициализация правил очистки"""
        
        # Словарь для исправления частых опечаток в медицинской/нутрициологической сфере
        self.typo_corrections = {
            # Медицинские термины
            "тейпирование": ["тейпированее", "тейпирванье", "тейпированиее", "тэйпирование"],
            "кинезиотейпинг": ["кинезиотейпинк", "кинезиотэйпинг", "кинезиотепинг"],
            "нутрициология": ["нутрициалогия", "нутрицеология", "нутрециология"],
            "витамины": ["витаминны", "витамыны", "вытамины"],
            "белки": ["бельки", "белкки"],
            "углеводы": ["углеводды", "углевоны", "угливоды"],
            "калории": ["каллории", "колории", "каллорий"],
            "диета": ["диэта", "диетта"],
            
            # Организационные термины
            "куратор": ["куротор", "кураттор", "куратар"],
            "экзамен": ["экзаммен", "экзамеен", "экзамне"],
            "сертификат": ["сертефикат", "сертификатт", "сертифекат"],
            "расписание": ["росписание", "расписанее", "распесание"],
            
            # Кулинарные термины
            "рецепт": ["рецепт", "рецеппт", "рецепр"],
            "ингредиенты": ["ингридиенты", "инградиенты", "ингредеенты"],
            "приготовление": ["преготовление", "приготовленее", "приготовлениее"],
            "майонез": ["майонэз", "майонейз", "маенез"],
            
            # Общие слова
            "помогает": ["помагает", "помогаеть", "помагаеть"],
            "полезно": ["полеззно", "полезнно", "полезен"],
            "можно": ["можжно", "мошно", "можео"],
            "нужно": ["нужжно", "нужнно", "нужео"],
            "когда": ["када", "кагда", "когдда"],
            "сколько": ["скольько", "сколка", "сколко"]
        }
        
        # Сленг и разговорная речь
        self.slang_corrections = {
            "норм": "нормально",
            "кул": "круто", 
            "супер": "отлично",
            "ок": "хорошо",
            "окей": "хорошо",
            "спс": "спасибо",
            "пжлст": "пожалуйста",
            "плз": "пожалуйста",
            "инфа": "информация",
            "инфо": "информация",
            "проф": "профессиональный",
            "макс": "максимальный",
            "мин": "минимальный",
            "комп": "компьютер",
            "чел": "человек",
            "челы": "люди",
            "мб": "может быть",
            "хз": "не знаю",
            "лол": "",  # удаляем
            "кек": "",  # удаляем
            "топ": "отличный",
            "фигня": "неважно",
            "крутяк": "отлично"
        }
        
        # Сокращения, которые нужно расшифровать
        self.abbreviation_expansions = {
            "др.": "другие",
            "т.д.": "так далее", 
            "т.п.": "тому подобное",
            "и т.д.": "и так далее",
            "и т.п.": "и тому подобное",
            "см.": "смотрите",
            "стр.": "страница",
            "гл.": "глава",
            "разд.": "раздел",
            "п.": "пункт",
            "пп.": "пункты",
            "н-р": "например",
            "напр.": "например",
            "мг": "миллиграмм",
            "г": "грамм", 
            "кг": "килограмм",
            "мл": "миллилитр",
            "л": "литр",
            "шт": "штук",
            "уп.": "упаковка",
            "ч.л.": "чайная ложка",
            "ст.л.": "столовая ложка",
            "ккал": "килокалории"
        }
        
        # Регулярные выражения для очистки
        self.cleaning_patterns = [
            # Удаление лишних пробелов
            (r'\s+', ' '),
            # Удаление пробелов перед знаками препинания
            (r'\s+([,.!?;:])', r'\1'),
            # Добавление пробелов после знаков препинания
            (r'([,.!?;:])(\w)', r'\1 \2'),
            # Удаление множественных знаков препинания
            (r'[.]{2,}', '.'),
            (r'[!]{2,}', '!'),
            (r'[?]{2,}', '?'),
            # Удаление специальных символов (кроме нужных)
            (r'[^\w\s\-.,!?;:()\[\]/%°]', ''),
            # Исправление дефисов
            (r'[-−–—]+', '-'),
            # Удаление лишних скобок
            (r'\(\s*\)', ''),
            (r'\[\s*\]', ''),
        ]
        
        # Стоп-слова для удаления малозначимых фраз
        self.stop_phrases = {
            "как известно", "всем известно", "очевидно", "понятно", 
            "итак", "таким образом", "следовательно", "в общем",
            "вообще говоря", "кстати", "между прочим", "например",
            "скажем", "допустим", "предположим", "возможно"
        }
        
    def clean_text(self, text: str, source_info: str = "") -> str:
        """
        Основная функция очистки текста
        
        Args:
            text: Исходный текст для очистки
            source_info: Информация об источнике (для логирования)
            
        Returns:
            Очищенный текст
        """
        if not text or not text.strip():
            return ""
        
        original_length = len(text)
        
        # 1. Нормализация Unicode
        text = self._normalize_unicode(text)
        
        # 2. Исправление регистра
        text = self._fix_case_issues(text)
        
        # 3. Исправление опечаток
        text = self._fix_typos(text)
        
        # 4. Исправление сленга
        text = self._fix_slang(text)
        
        # 5. Расшифровка сокращений
        text = self._expand_abbreviations(text)
        
        # 6. Очистка символов и форматирования
        text = self._clean_formatting(text)
        
        # 7. Удаление стоп-фраз
        text = self._remove_stop_phrases(text)
        
        # 8. Финальная очистка
        text = self._final_cleanup(text)
        
        cleaned_length = len(text)
        reduction_percent = ((original_length - cleaned_length) / original_length * 100) if original_length > 0 else 0
        
        if reduction_percent > 5:  # Логируем только значительные изменения
            logger.info(f"🧹 Очистка текста ({source_info}): {original_length} → {cleaned_length} символов (-{reduction_percent:.1f}%)")
        
        return text
    
    def _normalize_unicode(self, text: str) -> str:
        """Нормализация Unicode символов"""
        # Нормализация к NFC форме
        text = unicodedata.normalize('NFC', text)
        
        # Замена похожих символов
        replacements = {
            'ё': 'е',  # Замена ё на е для единообразия
            '№': 'номер',
            '§': 'параграф',
            '©': '',
            '®': '',
            '™': '',
            # Различные виды кавычек
            '"': '"', '"': '"', '„': '"', '‚': "'", ''': "'", ''': "'",
            # Различные виды дефисов
            '–': '-', '—': '-', '−': '-',
            # Математические символы
            '×': 'x', '÷': '/', '±': '+/-',
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        return text
    
    def _fix_case_issues(self, text: str) -> str:
        """Исправление проблем с регистром"""
        # Исправляем предложения, начинающиеся с маленькой буквы
        sentences = re.split(r'([.!?]\s+)', text)
        
        for i in range(0, len(sentences), 2):  # Каждое второе - это предложение
            if sentences[i]:
                # Первая буква предложения должна быть заглавной
                sentences[i] = sentences[i][0].upper() + sentences[i][1:] if len(sentences[i]) > 1 else sentences[i].upper()
        
        text = ''.join(sentences)
        
        # Исправляем аббревиатуры в верхнем регистре
        abbreviations = ['PDF', 'HTML', 'URL', 'API', 'FAQ', 'VIP', 'CEO', 'IT']
        for abbr in abbreviations:
            text = re.sub(f'\\b{abbr.lower()}\\b', abbr, text, flags=re.IGNORECASE)
        
        return text
    
    def _fix_typos(self, text: str) -> str:
        """Исправление опечаток"""
        text_lower = text.lower()
        
        for correct, typos in self.typo_corrections.items():
            for typo in typos:
                # Регистронезависимая замена с сохранением регистра
                pattern = re.compile(re.escape(typo), re.IGNORECASE)
                text = pattern.sub(correct, text)
        
        return text
    
    def _fix_slang(self, text: str) -> str:
        """Исправление сленга и разговорной речи"""
        words = text.split()
        
        for i, word in enumerate(words):
            word_clean = re.sub(r'[^\w]', '', word.lower())
            if word_clean in self.slang_corrections:
                replacement = self.slang_corrections[word_clean]
                if replacement:  # Если не пустая строка
                    words[i] = replacement
                else:  # Если нужно удалить
                    words[i] = ""
        
        return ' '.join(filter(None, words))
    
    def _expand_abbreviations(self, text: str) -> str:
        """Расшифровка сокращений"""
        for abbr, expansion in self.abbreviation_expansions.items():
            # Регистронезависимая замена
            pattern = r'\b' + re.escape(abbr) + r'\b'
            text = re.sub(pattern, expansion, text, flags=re.IGNORECASE)
        
        return text
    
    def _clean_formatting(self, text: str) -> str:
        """Очистка форматирования и символов"""
        for pattern, replacement in self.cleaning_patterns:
            text = re.sub(pattern, replacement, text)
        
        # Удаление строк с только цифрами или специальными символами
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if line and not re.match(r'^[\d\s\-.,()]+$', line):  # Не только цифры и знаки
                if len(line) > 3:  # Минимальная длина строки
                    cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def _remove_stop_phrases(self, text: str) -> str:
        """Удаление малозначимых фраз"""
        for phrase in self.stop_phrases:
            # Удаляем фразы в начале предложений
            pattern = r'\b' + re.escape(phrase) + r'[,\s]*'
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text
    
    def _final_cleanup(self, text: str) -> str:
        """Финальная очистка"""
        # Удаление множественных пробелов
        text = re.sub(r'\s+', ' ', text)
        
        # Удаление пробелов в начале и конце
        text = text.strip()
        
        # Удаление пустых строк
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        text = '\n'.join(lines)
        
        return text
    
    def remove_duplicates(self, texts: List[str]) -> List[str]:
        """
        Удаление дублирующихся текстов
        
        Args:
            texts: Список текстов
            
        Returns:
            Список уникальных текстов
        """
        if not texts:
            return []
        
        # Создаем множество для быстрого поиска дубликатов
        seen = set()
        unique_texts = []
        duplicates_count = 0
        
        for text in texts:
            # Нормализуем для сравнения (убираем пробелы, приводим к нижнему регистру)
            normalized = re.sub(r'\s+', ' ', text.strip().lower())
            
            if normalized and normalized not in seen:
                seen.add(normalized)
                unique_texts.append(text)
            else:
                duplicates_count += 1
        
        if duplicates_count > 0:
            logger.info(f"🗑️ Удалено {duplicates_count} дублирующихся текстов")
        
        return unique_texts
    
    def filter_quality_texts(self, texts: List[str], min_length: int = 50) -> List[str]:
        """
        Фильтрация текстов по качеству
        
        Args:
            texts: Список текстов
            min_length: Минимальная длина текста
            
        Returns:
            Отфильтрованный список текстов
        """
        quality_texts = []
        filtered_count = 0
        
        for text in texts:
            # Проверяем минимальную длину
            if len(text.strip()) < min_length:
                filtered_count += 1
                continue
            
            # Проверяем соотношение букв к символам
            letters = len(re.findall(r'[а-яё]', text.lower()))
            total_chars = len(text)
            letter_ratio = letters / total_chars if total_chars > 0 else 0
            
            if letter_ratio < 0.6:  # Минимум 60% букв
                filtered_count += 1
                continue
            
            # Проверяем, что это не служебная информация
            service_patterns = [
                r'^\d+$',  # Только цифры
                r'^стр\.\s*\d+',  # Номера страниц
                r'^глава\s*\d+',  # Заголовки глав
                r'^содержание',  # Оглавление
                r'^\.\.\.',  # Многоточия
            ]
            
            is_service = any(re.match(pattern, text.strip().lower()) for pattern in service_patterns)
            if is_service:
                filtered_count += 1
                continue
            
            quality_texts.append(text)
        
        if filtered_count > 0:
            logger.info(f"🔍 Отфильтровано {filtered_count} низкокачественных текстов")
        
        return quality_texts
    
    def clean_document_batch(self, documents: List[Dict]) -> List[Dict]:
        """
        Очистка пакета документов с метаданными
        
        Args:
            documents: Список документов с полями 'text' и 'metadata'
            
        Returns:
            Очищенный список документов
        """
        if not documents:
            return []
        
        logger.info(f"🧹 Начинаю очистку {len(documents)} документов...")
        
        cleaned_documents = []
        total_original_length = 0
        total_cleaned_length = 0
        
        for doc in documents:
            if 'text' not in doc:
                continue
            
            original_text = doc['text']
            original_length = len(original_text)
            total_original_length += original_length
            
            # Очищаем текст
            source_info = f"{doc.get('metadata', {}).get('source', 'Unknown')}"
            cleaned_text = self.clean_text(original_text, source_info)
            
            if cleaned_text.strip():  # Только если остался значимый текст
                doc_copy = doc.copy()
                doc_copy['text'] = cleaned_text
                cleaned_documents.append(doc_copy)
                total_cleaned_length += len(cleaned_text)
        
        # Удаляем дубликаты
        texts_for_dedup = [doc['text'] for doc in cleaned_documents]
        unique_texts = self.remove_duplicates(texts_for_dedup)
        
        # Восстанавливаем структуру документов
        final_documents = []
        for i, unique_text in enumerate(unique_texts):
            # Находим соответствующий документ
            for doc in cleaned_documents:
                if doc['text'] == unique_text:
                    final_documents.append(doc)
                    break
        
        # Фильтруем по качеству
        quality_texts = self.filter_quality_texts([doc['text'] for doc in final_documents])
        
        # Финальный список
        result_documents = []
        for quality_text in quality_texts:
            for doc in final_documents:
                if doc['text'] == quality_text:
                    result_documents.append(doc)
                    break
        
        # Статистика
        reduction_percent = ((total_original_length - total_cleaned_length) / total_original_length * 100) if total_original_length > 0 else 0
        
        logger.info(f"✅ Очистка завершена:")
        logger.info(f"   📄 Документов: {len(documents)} → {len(result_documents)}")
        logger.info(f"   📝 Объем текста: {total_original_length} → {total_cleaned_length} символов (-{reduction_percent:.1f}%)")
        logger.info(f"   🎯 Повышение качества: удалены опечатки, сленг, дубликаты")
        
        return result_documents
    
    def get_cleaning_stats(self, original_text: str, cleaned_text: str) -> Dict:
        """
        Получение статистики очистки
        
        Args:
            original_text: Исходный текст
            cleaned_text: Очищенный текст
            
        Returns:
            Словарь со статистикой
        """
        return {
            "original_length": len(original_text),
            "cleaned_length": len(cleaned_text),
            "reduction_percent": ((len(original_text) - len(cleaned_text)) / len(original_text) * 100) if len(original_text) > 0 else 0,
            "original_words": len(original_text.split()),
            "cleaned_words": len(cleaned_text.split()),
            "typos_fixed": sum(1 for typos in self.typo_corrections.values() for typo in typos if typo in original_text.lower()),
            "slang_fixed": sum(1 for slang in self.slang_corrections.keys() if slang in original_text.lower())
        }
