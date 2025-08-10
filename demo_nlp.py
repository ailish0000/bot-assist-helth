"""
🎯 Демонстрация NLP улучшений бота
"""

from nlp_processor_lite import NLPProcessor

def demo_nlp_improvements():
    """Демонстрирует улучшения в понимании вопросов"""
    
    print("🧠 ДЕМОНСТРАЦИЯ NLP УЛУЧШЕНИЙ БОТА")
    print("=" * 60)
    
    nlp = NLPProcessor()
    
    # Примеры разных формулировок одинаковых вопросов
    demo_questions = [
        ("у меня болит спина, тейпы помогут?", "medical_question"),
        ("спина ноет, поможет ли кинезиотейп?", "medical_question"),
        ("когда будет экзамен?", "organizational_question"),
        ("как связаться с куратором?", "organizational_question"),
        ("рецепт полезного салата", "recipe_question"),
        ("полезна ли киноа?", "product_question"),
        ("привет, как дела?", "greeting")
    ]
    
    for i, (question, expected_type) in enumerate(demo_questions, 1):
        print(f"\n{i}. СТУДЕНТ СПРАШИВАЕТ: '{question}'")
        print("-" * 50)
        
        # Анализируем вопрос
        analysis = nlp.analyze_question(question)
        
        # Показываем результат анализа
        print(f"🎯 Тип вопроса: {analysis.intent_type}")
        print(f"📊 Уверенность: {analysis.confidence:.2f}")
        print(f"🔑 Ключевые термины: {', '.join(analysis.keywords[:5])}")
        print(f"🗣️ Тон ответа: {analysis.response_tone}")
        
        # Показываем расширение поиска
        search_variants = nlp.enhance_search_query(question)
        print(f"🔍 Оригинальный поиск: {search_variants['original']}")
        print(f"🔍 Расширенный поиск: {search_variants['expanded'][:100]}...")
        
        # Проверяем правильность классификации
        if analysis.intent_type == expected_type:
            print("✅ Интент определен ПРАВИЛЬНО")
        else:
            print(f"❌ Ожидался {expected_type}, получен {analysis.intent_type}")

def demo_synonym_expansion():
    """Демонстрирует расширение синонимов"""
    
    print(f"\n\n🔍 ДЕМОНСТРАЦИЯ РАСШИРЕНИЯ СИНОНИМОВ")
    print("=" * 60)
    
    nlp = NLPProcessor()
    
    test_cases = [
        "боль в позвоночнике",
        "куратор не отвечает", 
        "рецепт майонеза",
        "тейпирование эффективно?"
    ]
    
    for question in test_cases:
        print(f"\n📝 Исходный вопрос: '{question}'")
        
        variants = nlp.enhance_search_query(question)
        
        print(f"🔍 Поиск с ключевыми словами: {variants['keywords_only']}")
        print(f"🔍 Поиск с подсказками: {variants['context_hints']}")
        print(f"🔍 Комбинированный поиск: {variants['combined'][:100]}...")

if __name__ == "__main__":
    try:
        demo_nlp_improvements()
        demo_synonym_expansion()
        
        print(f"\n\n🎉 ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА!")
        print(f"\n📈 КЛЮЧЕВЫЕ УЛУЧШЕНИЯ:")
        print("• Понимание разных формулировок одинаковых вопросов")
        print("• Классификация типов вопросов для персонализированных ответов")
        print("• Расширение поиска синонимами и контекстными подсказками")
        print("• Определение подходящего тона ответа")
        print("• Множественный поиск для лучшего покрытия информации")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        print("Проверьте, что файл nlp_processor_lite.py создан корректно")
