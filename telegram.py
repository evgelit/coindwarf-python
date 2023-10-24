import telebot
import env
from telebot import types
from services.analyze import Analyze

bot = telebot.TeleBot(env.env['telegram_token'])


@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    start_button = types.KeyboardButton("Start")
    markup.add(start_button)
    bot.send_message(message.from_user.id, "Say hello to my little friend", reply_markup=markup)


@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    if message.text == 'Start':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        six_rsi_btn = types.KeyboardButton('Get 6 period RSI')
        fourteen_rsi_btn = types.KeyboardButton('Get 14 period RSI')
        markup.add(fourteen_rsi_btn,
                   six_rsi_btn
                   )
        bot.send_message(message.from_user.id, 'At your service', reply_markup=markup)
    elif message.text == 'Get 14 period RSI':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        bot.send_message(message.from_user.id, 'Loading...', reply_markup=markup)
        signal = Analyze()
        signals = signal.execute(14)
        counter = 0
        for data in signals:
            if counter > 10:
                break
            bot.send_message(message.from_user.id, f"{str(data[1])}: {str(data[0])}", reply_markup=markup)
            counter += 1
    elif message.text == 'Get 6 period RSI':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        bot.send_message(message.from_user.id, 'Loading...', reply_markup=markup)
        signal = Analyze()
        signals = signal.execute(6)
        counter = 0
        for data in signals:
            if counter > 10:
                break
            bot.send_message(message.from_user.id, f"{str(data[1])}: {str(data[0])}", reply_markup=markup)
            counter += 1


bot.polling(none_stop=True, interval=0)