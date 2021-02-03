import numpy as np

friends = [
  {'name':'Bob','location':(5, 2, 10)},
  {'name':'David','location':(2, 3, 5)},
  {'name':'Mary','location':(19, 3, 4)},
  {'name':'Kevin','location':(3, 5, 1)}
]

def best_distance(friends):
    
    def calculateDistance(fromc, to):
    from_x, from_y, from_z = fromc[0], fromc[1], fromc[2]
    to_x, to_y, to_z = to[0], to[1], to[2]

    x = (to_x - from_x)**2 
    y = (to_y - from_y)**2
    z = (to_z - from_z)**2

    distance = (x + y + z) ** (1/2)

    return distance

    leastDistance = 10e100
    bestFriendToParty = ""

    for person in friends:
        name = person['name']
        coordinates = person['location']
        distances = []
        for friend in friends:
            frd = friend['name']
            location = friend['location']
            if(name != frd):
                distance = calculateDistance(coordinates, location)
                distances.append(distance)
            else:
                continue 
            person['distances'] = (np.mean(distances))
        if person['distances'] < leastDistance:
            leastDistance = person['distances']
            bestFriendToParty = person['name']
            
        return print("The best friend to host the party is {}, average distance is {}".format(bestFriendToParty, leastDistance))