import vk_api
from py2neo import Graph, Node, Relationship


def deep_friends(user_id=0, depth=0):
    # Получаем информацию о текущем пользователе
    if not user_id:
        person = vk.method('users.get')
        user_id = person[0]['id']
    else:
        person = vk.method('users.get', {'user_ids': user_id})

    person_name = person[0]['first_name']
    person_id = str(person[0]['id'])

    # Записываем Node в neo4j
    if person_node_not_exist(graph, person_id):
        graph.create(Node("Person", person_id=person_id, person_name=person_name))

    # Получаем список друзей текущего пользователя
    friends = vk.method('friends.get', {'user_id': user_id})

    # Получаем информацию о друзьях текущего пользователя
    friends_info = vk.method('users.get', {'user_ids': ','.join([str(i) for i in friends['items']])})

    for friend in friends_info:
        friend_name = friend['first_name']
        friend_id = str(friend['id'])
        if person_node_not_exist(graph, friend_id):
            # Записываем Node в neo4j
            graph.create(Node('Person', person_id=friend_id, person_name=friend_name))
            # Записываем Relationship в neo4j
            check_and_add_relation(graph, person_id, friend_id, "knows")
        try:
            # Получаем информацию о связях текущего друга
            mutuals = vk.method('friends.getMutual', {'source_uid': person_id, 'target_uid': friend['id']})
            mutuals_info = vk.method('users.get', {'user_ids': ','.join([str(i) for i in mutuals])})
            for mutual in mutuals_info:
                mutual_name = mutual['first_name']
                mutual_id = str(mutual['id'])
                # Записываем Node в neo4j
                if person_node_not_exist(graph, mutual_id):
                    graph.create(Node('Person', person_id=mutual_id, person_name=mutual_name))
                # Записываем Relationship в neo4j
                check_and_add_relation(graph, friend_id, mutual_id, "knows")
        except:
            print('Cant get mutuals of id' + str(person_id), 'with id' + str(friend['id']))

    # Если глубина не равна 0 - вызываем функцию для всех друзей текущего пользователя
    if depth == 0:
        return
    else:
        for friend in friends['items']:
            try:
                deep_friends(friend, depth - 1)
            except:
                print('Cant get friends of id' + str(friend))

# Проверяем, есть ли пользователь в графе
def person_node_not_exist(param_graph, person_id):
    result_query = param_graph.run("MATCH (p:Person) WHERE p.person_id=$person_id RETURN p",
                                   parameters={'person_id': person_id})
    if str(result_query) == '(No data)':
        return True
    else:
        return False

# Проверяем, есть ли связь в графе и добавляем
def check_and_add_relation(graph, node1_id, node2_id, rel_type):
    query = f'''
    MATCH (a:Node),(b:Node)
    WHERE a.id = '{node1_id}' AND b.id = '{node2_id}'
    OPTIONAL MATCH (a)-[r:{rel_type}]-(b)
    RETURN r
    '''
    result = graph.run(query).evaluate()

    if not result:
        node1 = graph.nodes.match("Person", person_id=node1_id).first()
        node2 = graph.nodes.match("Person", person_id=node2_id).first()

        rel = Relationship(node1, rel_type, node2)
        graph.create(rel)
        print('Relationship created')


if __name__ == "__main__":
    token = 'vk1.a.f1ZT4J6PqjZC9M45J_L-3hWOikHZCKrxq9H2IbhbxXqjPvFztpIVuS9b2xEhYzIo2AUaPp47O-dNbKubjyWt6VV03QOuj9kQwZNOIW6455sv5Lo9qwJXQ_OUKuVuM809CmwjSPxtVLh9Sg3Y54ZJxsFYzbzoICNKsWkt5v5Mv93m4ccEAaeWZstRfJu3tg-vRWBFF1kEFOKW7bIB3PvF8g'
    app_id = 51794060
    user_id = 0  # id пользователя, для которого необходимо построить граф (0 - текущий пользователь)
    depth = 1  # глубина рекурсивного прохода по друзьям

    # авторизация Вк
    # Мой id 332689673
    vk = vk_api.VkApi(token=token)
    vkU = vk.get_api()

    # Подлючение к neo4j
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j1234"))

    # рекурсивный проход по друзьям, друзьям друзей и т.д.
    deep_friends(user_id, depth)
