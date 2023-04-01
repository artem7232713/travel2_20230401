import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# typegroup_list=['amenity','leisure','nature']
typegroup_list = ['amenity', 'leisure', 'nature', 'abutters', 'aeroway', 'shop', 'sport', 'surface', 'tourism',
                  'barrier', 'boundary', 'craft', 'emergency', 'highway', 'information',
                  'landuse', 'leisure', 'contact', 'military', 'natural', 'power',
                  'railway', 'references','None']



def getTypegroup(el):
    for i in el.tags:
        typegroup = 'None'
        if typegroup_list.count(i) != 0:  # поиск группы типов
            typegroup = i
            break
    return typegroup
def insertRow(el,cursor):
    lat = el.lat
    lon = el.lon
    osmid = el.id
    charList = el.tags
    typegroup = getTypegroup(el)
    if typegroup!='None':
        typ = charList[typegroup]
        name = 'Name'
        cursor.execute('''select * from insert_object_type('{}','{}',0)'''.format(
            typegroup, typ))
        try:
            name = el.tags['name']
            if   "'" in name:
                    name = name.replace("'"," ")
        except:
            rcmkntlv=1
        id_type = cursor.fetchall()[0][0]
        cursor.execute('''select * from insert_object({},{},{},{},'{}',0)'''.format(
            id_type,osmid,lat,lon,name))
        id_object = cursor.fetchall()[0][0]
        for key in charList:
            if key not in typegroup_list:
                cursor.execute('''select * from insert_characteristic({},'{}',0)'''.format(
                id_type,key))
                id_char = cursor.fetchall()[0][0]
                
                value = charList[key]
                if   "'" in value:
                    value = value.replace("'"," ")
                cursor.execute('''select * from insert_object_characteristic({},{},'{}',0)'''.format(id_object,
                                                                                                   id_char,value))
    
    
    












def insertType(typel, cursor):
    req1 = '''select * from get_id_object_type('{}','{}',0)'''.format(
        typel[1], typel[0])  # поиск типа в БД
    # print(h)
    # print(a)
    cursor.execute(req1)
    r = cursor.fetchall()[0][0]
    if r == -1:  # если не найден,добавить тип
        req2 = '''
        INSERT INTO public."object_types" (object_type,object_typegroup)
        VALUES ('{}','{}');
        select currval('object_types_id_object_type_seq')'''.format(typel[0], typel[1])
        cursor.execute(req2)
        return cursor.fetchall()[0][0]  # наполнение typechar
    else:
        return r


def insertTag(i, j, cursor, typeChar):
    h = '''select * from get_id_characteristic('{}',{},0)'''.format(
        j, typeChar[i])

    cursor.execute(h)
    r = cursor.fetchall()[0][0]
    # print(r)
    if r == -1:
        a = '''
        INSERT INTO public."characteristics" (characteristic,id_object_type)
	VALUES ('{}',{});'''.format(j, typeChar[i])
        cursor.execute(a)


def insertObject(el, cursor, typeChar, tagList):

    a = '''select * from get_id_osm({},0)'''.format(el.id)
    cursor.execute(a)
    ed = cursor.fetchall()[0][0]
    typegroup = 'None'
    for h in el.tags:
        if typegroup_list.count(h) != 0:  # поиск группы типов
            typegroup = h
            break
    if ed == -1:

        if typegroup != 'None':
            k = (el.tags[typegroup], typegroup)

        else:
            k = ('None', 'None')
        id_object_type = typeChar[k]

        if el.tags.get('name', -1) != -1:
            name = el.tags['name'].replace("'", "''")
            print(name)
            a = '''INSERT INTO public.objects (id_osm,id_object_type,name,latitude,longitude)
            VALUES ({},{},'{}',{},{});
            select currval('objects_id_object_seq');'''.format(el.id, id_object_type, name, el.lat, el.lon)
        else:
            a = '''INSERT INTO public.objects (id_osm,id_object_type,latitude,longitude)
            VALUES ({},{},{},{});
            select currval('objects_id_object_seq');'''.format(el.id, id_object_type, el.lat, el.lon)
    # print(a)

            cursor.execute(a)

            id_object = cursor.fetchall()[0]


def addTag(cursor, tagList, k):
    if k != ('None', 'None'):

        for tag in tagList[k]:
            a = '''select * from get_id_object_type('{}','{}',0)'''.format(
                k[1], k[0])
            cursor.execute(a)
            type_id = cursor.fetchall()[0]
            a = '''select * from get_id_characteristic('{}',{},0)'''.format(
                tag, type_id[0])
            cursor.execute(a)
            tag_id = cursor.fetchall()[0]
            if tag_id[0] != -1:
                a = '''
INSERT INTO public.object_characteristics (id_object,id_characteristic,value)
	VALUES ({},{},'{}');'''.format(id_object[0], tag_id[0], el.tags[tag])
        cursor.execute(a)
