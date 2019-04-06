import pymysql
import logging

conn = pymysql.connect(host='localhost',
                       user='root',
                       password='',
                       db='advanced_python')

logging.basicConfig(filename="sample.log", level=logging.INFO, filemode="w")


def query_set(query, value_query, outline=None, flag=True):
    with conn.cursor() as cursor:
        try:
            logging.info(query)
            logging.info(value_query)
            cursor.execute(query, value_query)
        except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                pymysql.IntegrityError, TypeError) as err:
            print('Database reset error: ', err.args[1])
        if outline == 'one':
            table = cursor.fetchone()
        elif outline == 'many':
            table = cursor.fetchall()
        else:
            conn.commit()
            return None
    if table is None and flag==True:
        print('TypeError: Database reset None')
    return table


class Field:
    def __init__(self, f_type, required=True, default=None):
        self.f_type = f_type
        self.required = required
        self.default = default

    def validate(self, value):
        if value is None and not self.required:
            return None

        if self.f_type != type(value) and self.f_type != bool:
            raise TypeError
        try:
            self.f_type(value)
        except:
            raise TypeError
        return self.f_type(value)


class IntField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(int, required, 0)


class StringField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(str, required, '')


class BoolField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(bool, required, False)


class FloatField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(float, required, 0)


class ModelMeta(type):
    def __new__(mcs, name, bases, namespace):
        if name == 'Model':
            return super().__new__(mcs, name, bases, namespace)

        meta = namespace.get('Meta')
        if meta is None:
            raise ValueError('meta is none')
        if not hasattr(meta, 'table_name'):
            raise ValueError('table_name is empty')

        for base in bases:
            for m in base.__mro__:
                if m == eval('Model'):
                    break
                else:
                    namespace = {**m.__dict__, **namespace}

        fields = {k: v for k, v in namespace.items()
                  if isinstance(v, Field)}
        namespace['_fields'] = fields
        namespace['_table_name'] = meta.table_name
        return super().__new__(mcs, name, bases, namespace)


class Manage:
    def __init__(self):
        self.model_cls = None
        self.list_object = []

    def __get__(self, instance, owner):
        self.model_obj = instance
        self.model_cls = owner
        return self

    def all(self, condition=None):
        if condition is not None:
            query_condition = 'WHERE {}'.format(' AND '.join('{}{}%s'.format(i.name, i.operation) for i in condition))
            value_query = list(i.value for i in condition)
        else:
            query_condition = ''
            value_query = None
        query = 'SELECT * FROM {} '.format(self.model_cls.Meta.table_name) + query_condition
        tables = query_set(query, value_query, 'many')
        dict_table = {}  # поле и значение объекта
        many_model = ManyModel()
        for table_index in range(len(tables)):
            for key, val in zip(self.model_cls._fields.keys(), tables[table_index]):
                dict_table[key] = val
            many_model.list_model.append(self.model_cls(**dict_table))
            for field_name, field in self.model_cls._fields.items():
                value = field.validate(dict_table.get(field_name))
                setattr(many_model.list_model[table_index], field_name, value)
        return many_model

    def get(self, flag=True, **kwargs):
        for item in kwargs.keys():
            if item not in self.model_cls._fields.keys():
                raise AttributeError
        query = "SELECT * FROM {} WHERE {}".format(self.model_cls.Meta.table_name,
                                                   ' AND '.join('{}=%s'.format(x) for x in kwargs.keys()))
        value_query = list(x for x in kwargs.values())
        table = query_set(query,value_query, 'one', flag)
        if table is None:
            return None
        kwargs = {}
        for key, val in zip(self.model_cls._fields.keys(), table):
            kwargs[key] = val
        return self.model_cls(**kwargs)

    def create(self, **kwargs):
        for field_name, field in self.model_cls._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self.model_cls, field_name, value)
            kwargs[field_name] = value
        query = "INSERT INTO {} ({}) VALUES ({})".format(self.model_cls.Meta.table_name,
                                                         ', '.join(str(x) for x in kwargs.keys()),
                                                         ', '.join('%s' for _ in range(len(kwargs))))
        value_query = list(x for x in kwargs.values())
        query_set(query, value_query)
        return self.model_cls

    def save(self):
        field_names = {}
        for field_name in self.model_cls._fields.keys():
            field_names[field_name] = ''
        if self.model_obj is not None:
            for key in field_names.keys():
                field_names[key] = getattr(self.model_obj, key)
        else:
            return None
        if self.get(id=self.model_obj.id, flag=False) == None:
            self.create(**field_names)
        else:
            self.update(**field_names)

    def delete(self, condition=None):
        if condition is None:
            query_condition = 'id={}'.format(self.model_obj.id)
            value_query = None
        else:
            query_condition = '{}'.format(' AND '.join('{}{}%s'.format(i.name, i.operation) for i in condition))
            value_query = list(i.value for i in condition)
        query = "DELETE FROM {} WHERE ({})".format(self.model_cls.Meta.table_name, query_condition)
        query_set(query, value_query)

    def update(self, condition=None, **kwargs):
        try:
            self_update = self.model_cls
        except AttributeError:
            return None
        if condition is None:
            query_condition = 'id={}'.format(self.model_obj.id)
            value_query = []
        else:
            query_condition = '{}'.format(' AND '.join('{}{}%s'.format(i.name, i.operation) for i in condition))
            value_query = list(i.value for i in condition)
        set_value = ', '.join("{}=%s".format(x) for x in kwargs.keys())
        value_query_start = list(i for i in kwargs.values())
        value_query = value_query_start + value_query

        query = "UPDATE {} SET  {} WHERE ({})".format(self_update.Meta.table_name, set_value, query_condition)
        query_set(query, value_query)
        for field_name, value in kwargs.items():
            setattr(self_update, field_name, value)
        return self

    def filter(self, **kwargs):
        return ManyModel(manage_self=self, **kwargs)


class Condition:
    term_dict = {'lt': '<', 'le': '<=','gt': '>', 'ge': '>='}

    def __init__(self, name, value):
        if name[-2:] in Condition.term_dict.keys() and name[-4:-2] == '__':
            self.name = name[:-4]
            self.operation = Condition.term_dict[name[-2:]]
            self.value = value
        else:
            self.name = name
            self.operation = '='
            self.value = value


class ManyModel:
    def __init__(self, manage_self=None, **condition):
        self.list_model = []
        self.condition = []
        self.manage_self = manage_self
        if condition is not None:
            self.filter(**condition)

    def __getitem__(self, item):
        if type(item) == slice:
            self.list_model = self.list_model[item]
        else:
            self.item = item
            if item >= len(self.list_model):
                return None
            return self.list_model[item]

    def __iter__(self):
        self.item = -1
        return self

    def __next__(self):
        self.item = self.item + 1
        if self.item == len(self.list_model):
            raise StopIteration
        else:
            return self.list_model[self.item]

    def update(self, **kwargs):
        if self.condition is None:
            for i in self.list_model:
                i.update(**kwargs)
        else:
            Manage.update(self.manage_self, self.condition, **kwargs)
            self.condition = None
        return self

    def delete(self):
        if self.condition is None:
            for i in self.list_model:
                i.delete()
        else:
            Manage.delete(self.manage_self, self.condition)
            self.condition = None

    def save(self):
        for i in self.list_model:
            i.save()

    def all(self):
        if self.condition is None:
            raise AttributeError
        return Manage.all(self.manage_self, self.condition)

    def filter(self, **condition):
        for condition_name, condition_value in condition.items():
            self.condition.append(Condition(condition_name, condition_value))
        return self


class Model(metaclass=ModelMeta):
    class Meta:
        table_name = ''

    objects = Manage()

    def __init__(self, *_, **kwargs):
        for field_name, field in self._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self, field_name, value)

    def save(self):
        return self.objects.save()

    def delete(self):
        return self.objects.delete()

    def update(self, **kwargs):
        return self.objects.update(**kwargs)


class User(Model):
    id = IntField()
    name = StringField()
    sex = BoolField()
    height = FloatField()

    class Meta:
        table_name = 'Users'


class Man(User, Model):
    age = IntField()

    class Meta:
        table_name = 'Man'


# Создание строки в таблице
# create - создание объекта
#
# User.objects.create(id=2, name='Иванова', sex=False, height=155.0)
# User.objects.create(id=3, name='Сидоров', sex=True, height=170.5)
# User.objects.create(id=5, name='Иванов', sex=False, height=160.0)
#
# Создание экземпляра
#
# user = User(id=5, name='Петров', sex=True, height=147.1)
# user.save()
#
# user = User(id=4, name='Петров', sex=True, height=147.1)
# user.name='Change'
# user.save()
#
# Извлечение данных через get
#
# user = User.objects.get(name='Петров', sex='1')
# print(user.__dict__)
# user.delete()
#
# man = Man.objects.get(id=1)
# print(man.__dict__)
#
#
# Извлечение данных через .all()
#
# user = User.objects.all()
#
# user = User.objects.filter(id__gt=1).all()
# for u in user:
#     print(u.id, u.name)
#
#
# Метод обновления update()
#
# user = User.objects.get(id=1)
# user.update(name='Change')
#
# для delete
#
# user = User.objects.get(id=1)
# user.delete()
#
# man = Man.objects.create(id=1, name=12, sex='0', height=180.1, age=22)

# user = User.objects.filter(name='name2').all().update(name='name')
conn.close()

