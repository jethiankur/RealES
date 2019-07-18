from utils import prettify_json, parse_json, LivyRequests
import time


def get_idle_session_url():

    livy_host = "http://localhost:8998"
    sessions= LivyRequests().list_sessions()
    time.sleep(3)
    idle_session=[]
    print(sessions)    
    if len(sessions) > 0 :
        itr = 0
        for session in sessions:
            print("hello")
            print(type(session))
            print(session["state"])
            if session["state"] == 'idle':
                idle_session.append(session["id"])
            itr=itr+1
    if len(idle_session) > 0 :
        #will check on how to get the best session considering diff parameters  like what is in the memorys
        session_url = livy_host + "/sessions/{}".format(idle_session[0])
        print (session_url)
        return session_url
    else:
        spark_info = LivyRequests().run_session()
        session_url = spark_info["session-url"]
    created_idle_session=[]
    while(True):
        results = sessions = LivyRequests().list_sessions()
        itr=0
        time.sleep(3)
        print(sessions)
        for session in sessions:
            if session["state"] == 'idle':
                created_idle_session.append(session["id"])
                break
            itr=itr+1
        if len(created_idle_session) > 0:
            break
    if len(created_idle_session) > 0:
        session_url = livy_host + "/sessions/{}".format(created_idle_session[0])
        print (session_url)
        return session_url
    else:
        print("Contact support")

#if __name__=='__main__':
#    get_idle_session_url()

