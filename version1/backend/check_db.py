from cores.dbconnection.mongo import get_db
db = get_db()
doc = db.app_config.find_one({'_key': 'system_settings'})
print(doc.keys() if doc else None)
if doc:
    print('v2_extract_prompt in db:' , 'v2_extract_prompt' in doc)
    if 'v2_extract_prompt' in doc:
        print('length:', len(doc['v2_extract_prompt']))
