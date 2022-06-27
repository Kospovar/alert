import telegram
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
from read_db.CH import Getch
import scipy.stats

sns.set(font_scale = 1)

def check_anomaly_qn(df, metric, a_qn=4, n_qn=5):
    df['q25'] = df[metric].shift(1).rolling(n_qn).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n_qn).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up_qn'] = df['q75'] + a_qn * df['iqr']
    df['low_qn'] = df['q25'] - a_qn * df['iqr']
    
    df['up'] = df['up_qn'].rolling(n_qn, center=True, min_periods=1).mean()
    df['low'] = df['low_qn'].rolling(n_qn, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low_qn'].iloc[-1] or df[metric].iloc[-1] > df['up_qn'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

def check_anomaly_std(df, metric, a=2.6, n=8):
    df['roll_mean'] = df[metric].shift(1).rolling(n).mean()
    df['roll_std'] = df[metric].shift(1).rolling(n).std()
    df['up_std'] =  df['roll_mean'] + (a * df['roll_std'])
    df['low_std'] =  df['roll_mean'] - (a * df['roll_std'])
    
    df['up'] = df['up_std'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low_std'].rolling(n, center=True, min_periods=1).mean()  
    
    if df[metric].iloc[-1] < df['low_std'].iloc[-1] or df[metric].iloc[-1] > df['up_std'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

def check_anomaly_confidence(df, metric, n_conf=4, n=5):
    df['roll_mean'] = df[metric].shift(1).rolling(n_conf, min_periods=1).mean()
    df['roll_sem'] = df[metric].shift(1).rolling(n_conf, min_periods=1).sem()
    df['h'] =  df['roll_sem'] * scipy.stats.t.ppf(1.99 / 2, n_conf-1)
    df['up_conf'] = df['roll_mean'] + (df['h'])
    df['low_conf'] = df['roll_mean'] -  (df['h'])
    
    df['up'] = df['up_conf'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low_conf'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low_conf'].iloc[-1] or df[metric].iloc[-1] > df['up_conf'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

def run_alert(chat = None):
    chat_id = chat or 491009072
    bot = telegram.Bot(token='5332579519:AAFPJ9lJrVM32xiSwWfOT_U7f8ESsOd88Vk')
    
    feed = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as time
                        , uniqExact(user_id) as DAU
                        , (countIf(user_id, action='like')/countIf(user_id, action='view')) as CTR
                        , countIf(user_id, action='like') as like
                        , countIf(user_id, action='view') as view
                    FROM simulator_20220520.feed_actions
                    WHERE time >=  today() -1 and time < toStartOfFifteenMinutes(now())
                    GROUP BY time
                    ORDER BY time ''').df

    message = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as time
                        , count(DISTINCT user_id) messages
                    FROM simulator_20220520.message_actions
                    WHERE time >=  today() - 1 and time < toStartOfFifteenMinutes(now())
                    GROUP BY time
                    ORDER BY time''').df
    
    t = pd.merge(feed, message, on = 'time')
    
    metrics = ['DAU', 'CTR', 'like', 'view', 'messages']
    
    for metric in metrics:
        df = t[['time',metric]].copy()
        
        if metric in ['CTR']:
            is_alert, df = check_anomaly_std(df, metric)
        else:
            is_alert, df = check_anomaly_confidence(df, metric)
        
        
        if is_alert == 1:
            
            msg = '''Метрика {metric}:\nТекущее значение = {cur_value:.2f}\nОтклонение от предыдущего значения {diff}
            '''.format(metric=metric,cur_value=df[metric].iloc[-1],diff=round(1-(df[metric].iloc[-1]/df[metric].iloc[-2]),2))


            
            sns.set(rc={'figure.figsize' : (14, 16)})
            plt.tight_layout()
            ax = sns.lineplot(x = df['time'], y=df[metric], label='metric')
            ax = sns.lineplot(x = df['time'], y=df['low'], label='low')
            ax = sns.lineplot(x = df['time'], y=df['up'], label='up')

            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)
            
            ax.set_title('{}'.format(metric))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendPhoto(chat_id = chat_id, photo = plot_object, caption = msg)

        
try:
    run_alert()
except Exception as e:
    print(e)