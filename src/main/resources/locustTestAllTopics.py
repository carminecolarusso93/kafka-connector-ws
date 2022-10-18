from locust import HttpUser, TaskSet, task, between,events, constant
import websocket
from websocket import create_connection
import time
import random
import gevent

from uuid import uuid4

'''
python3 -m locust -f locustTest.py --host ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector/ --users 100 --spawn-rate 100 --run-time 1m --headless --html reports/8marzo/report-BASE-100user-100spawnrate-1pod-4core-1minutes.html
'''
class UserBehavior(TaskSet):

    def on_start(self):
        uri = "ws://connector-kafka-ws-promenade-lyon.apps.kube.rcost.unisannio.it/java-websocket/kafka-connector/"
        #websocket.enableTrace(True)
        self.ws = create_connection(uri)
        self.ws.send("Lyon_1er_Arrondissement-Northbound,Lyon_5e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
        
        def _receive():
            
            while True:
                res = self.ws.recv()
                '''
                start_at = time.time()
                end_at = time.time()
                response_time = int((end_at - start_at) * 1000000)
                events.request_success.fire(
                    request_type='WebSocket Recv',
                    name='test/ws/chat',
                    response_time=response_time,
                    response_length=len(res),
                )
                '''
                #print(res)
                

        gevent.spawn(_receive)

        
    def on_quit(self):
        self.ws.disconnect()

    

    @task
    def other_message1(self):
        
        start_at = time.time()
        
        #self.environment.runner.quit()
        
        self.ws.send("Albigny-sur-Saone-Northbound,Alix-Northbound,Amberieux-Northbound,Amberieux-en-Dombes-Northbound,Ampuis-Northbound,Anse-Northbound,Anthon-Northbound,Ars-sur-Formans-Northbound,Artas-Northbound,Bagnols-Northbound,Balan-Northbound,Beauvoir-de-Marc-Northbound,Beligneux-Northbound,Belmont-d_Azergues-Northbound,Bessenay-Northbound,Beynost-Northbound,Birieux-Northbound,Bonnefamille-Northbound,Bourg-Saint-Christophe-Northbound,Bressolles-Northbound,Brignais-Northbound,Brindas-Northbound,Bron-Northbound,Bully-Northbound,Cailloux-sur-Fontaines-Northbound,Caluire-et-Cuire-Northbound,Chalamont-Northbound,Chamagnieu-Northbound,Champagne-au-Mont-d_Or-Northbound,Chaponnay-Northbound,Chaponost-Northbound,Charantonnay-Northbound,Charbonnieres-les-Bains-Northbound,Charly-Northbound,Charnay-Northbound,Charvieu-Chavagneux-Northbound,Chassagny-Northbound,Chasse-sur-Rhone-Northbound,Chasselay-Northbound,Chassieu-Northbound,Chateauneuf-Northbound,Chatillon-Northbound,Chaussan-Northbound,Chavanoz-Northbound,Chazay-d_Azergues-Northbound,Chessy-Northbound,Chevinay-Northbound,Chuzelles-Northbound,Civrieux-Northbound,Civrieux-d_Azergues-Northbound,Cogny-Northbound,Collonges-au-Mont-d_Or-Northbound,Colombier-Saugnieu-Northbound,Communay-Northbound,Corbas-Northbound,Courzieu-Northbound,Couzon-au-Mont-d_Or-Northbound,Crans-Northbound,Craponne-Northbound,Curis-au-Mont-d_Or-Northbound,Dagneux-Northbound,Dardilly-Northbound,Dargoire-Northbound,Decines-Charpieu-Northbound,Denice-Northbound,Diemoz-Northbound,Dommartin-Northbound,Echalas-Northbound,Ecully-Northbound,Estrablin-Northbound,Eveux-Northbound,Faramans-Northbound,Feyzin-Northbound,Fleurieu-sur-Saone-Northbound,Fleurieux-sur-l_Arbresle-Northbound,Fontaines-Saint-Martin-Northbound,Fontaines-sur-Saone-Northbound,Francheville-Northbound,Frans-Northbound,Frontenas-Northbound,Frontonas-Northbound,Genas-Northbound,Genay-Northbound,Genilac-Northbound,Givors-Northbound,Gleize-Northbound,Grenay-Northbound,Grezieu-la-Varenne-Northbound,Grigny-Northbound,Heyrieux-Northbound,Irigny-Northbound,Janneyrias-Northbound,Jarnioux-Northbound,Jassans-Riottier-Northbound,Jonage-Northbound,Jons-Northbound,Joyeux-Northbound,L_Arbresle-Northbound,La_Boisse-Northbound,La_Mulatiere-Northbound,La_Tour-de-Salvagny-Northbound,La_Verpilliere-Northbound,Lacenas-Northbound,Lachassagne-Northbound,Lapeyrouse-Northbound,Le_Bois-d_Oingt-Northbound,Le_Breuil-Northbound,Le_Montellier-Northbound,Legny-Northbound,Lentilly-Northbound,Les_Cheres-Northbound,Les_Haies-Northbound,Liergues-Northbound,Limas-Northbound,Limonest-Northbound,Lissieu-Northbound,Loire-sur-Rhone-Northbound,Longes-Northbound,Lorette-Northbound,Lozanne-Northbound,Lucenay-Northbound,Luzinay-Northbound,Lyon_1er_Arrondissement-Northbound,Lyon_2e_Arrondissement-Northbound,Lyon_3e_Arrondissement-Northbound,Lyon_4e_Arrondissement-Northbound,Lyon_5e_Arrondissement-Northbound,Lyon_6e_Arrondissement-Northbound,Lyon_7e_Arrondissement-Northbound,Lyon_8e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound,Marcilly-d_Azergues-Northbound,Marcy-Northbound,Marcy-l_Etoile-Northbound,Marennes-Northbound,Massieux-Northbound,Messimy-Northbound,Meyzieu-Northbound,Millery-Northbound,Mionnay-Northbound,Mions-Northbound,Miribel-Northbound,Miserieux-Northbound,Moidieu-Detourbe-Northbound,Moire-Northbound,Montagny-Northbound,Montanay-Northbound,Monthieux-Northbound,Montluel-Northbound,Morance-Northbound,Mornant-Northbound,Neuville-sur-Saone-Northbound,Neyron-Northbound,Nievroz-Northbound,Oingt-Northbound,Orlienas-Northbound,Oullins-Northbound,Oytier-Saint-Oblas-Northbound,Parcieux-Northbound,Perouges-Northbound,Pierre-Benite-Northbound,Pizay-Northbound,Poleymieux-au-Mont-d_Or-Northbound,Pollionnay-Northbound,Pommiers-Northbound,Pont-Eveque-Northbound,Pont-de-Cheruy-Northbound,Pouilly-le-Monial-Northbound,Pusignan-Northbound,Quincieux-Northbound,Rance-Northbound,Reyrieux-Northbound,Rignieux-le-Franc-Northbound,Rillieux-la-Pape-Northbound,Rive-de-Gier-Northbound,Riverie-Northbound,Rivolet-Northbound,Roche-Northbound,Rochetaillee-sur-Saone-Northbound,Rontalon-Northbound,Sain-Bel-Northbound,Saint-Andeol-le-Chateau-Northbound,Saint-Andre-de-Corcy-Northbound,Saint-Andre-la-Cote-Northbound,Saint-Bernard-Northbound,Saint-Bonnet-de-Mure-Northbound,Saint-Cyr-au-Mont-d_Or-Northbound,Saint-Cyr-sur-le-Rhone-Northbound,Saint-Didier-au-Mont-d_Or-Northbound,Saint-Didier-de-Formans-Northbound,Saint-Didier-sous-Riverie-Northbound,Saint-Eloi-Northbound,Saint-Fons-Northbound,Saint-Genis-Laval-Northbound,Saint-Genis-les-Ollieres-Northbound,Saint-Georges-d_Esperanche-Northbound,Saint-Germain-Nuelles-Northbound,Saint-Germain-au-Mont-d_Or-Northbound,Saint-Jean-de-Bournay-Northbound,Saint-Jean-de-Thurigneux-Northbound,Saint-Jean-de-Touslas-Northbound,Saint-Jean-des-Vignes-Northbound,Saint-Joseph-Northbound,Saint-Just-Chaleyssin-Northbound,Saint-Laurent-d_Agny-Northbound,Saint-Laurent-de-Mure-Northbound,Saint-Marcel-Northbound,Saint-Martin-en-Haut-Northbound,Saint-Martin-la-Plaine-Northbound,Saint-Maurice-de-Beynost-Northbound,Saint-Maurice-de-Gourdans-Northbound,Saint-Maurice-sur-Dargoire-Northbound,Saint-Pierre-de-Chandieu-Northbound,Saint-Pierre-la-Palud-Northbound,Saint-Priest-Northbound,Saint-Quentin-Fallavier-Northbound,Saint-Romain-au-Mont-d_Or-Northbound,Saint-Romain-en-Gal-Northbound,Saint-Romain-en-Gier-Northbound,Saint-Sorlin-Northbound,Saint-Symphorien-d_Ozon-Northbound,Sainte-Catherine-Northbound,Sainte-Colombe-Northbound,Sainte-Consorce-Northbound,Sainte-Croix-Northbound,Sainte-Euphemie-Northbound,Sainte-Foy-les-Lyon-Northbound,Sarcey-Northbound,Sathonay-Camp-Northbound,Sathonay-Village-Northbound,Satolas-et-Bonce-Northbound,Savigneux-Northbound,Savigny-Northbound,Septeme-Northbound,Serezin-du-Rhone-Northbound,Serpaize-Northbound,Seyssuel-Northbound,Simandres-Northbound,Solaize-Northbound,Soucieu-en-Jarrest-Northbound,Sourcieux-les-Mines-Northbound,Taluyers-Northbound,Tartaras-Northbound,Tassin-la-Demi-Lune-Northbound,Ternay-Northbound,Theize-Northbound,Thil-Northbound,Thurins-Northbound,Tignieu-Jameyzieu-Northbound,Toussieu-Northbound,Toussieux-Northbound,Tramoyes-Northbound,Treves-Northbound,Trevoux-Northbound,Tupin-et-Semons-Northbound,Valencin-Northbound,Vaugneray-Northbound,Vaulx-Milieu-Northbound,Vaulx-en-Velin-Northbound,Venissieux-Northbound,Vernaison-Northbound,Versailleux-Northbound,Vienne-Northbound,Villars-les-Dombes-Northbound,Ville-sur-Jarnioux-Northbound,Villefontaine-Northbound,Villefranche-sur-Saone-Northbound,Villette-d_Anthon-Northbound,Villette-de-Vienne-Northbound,Villeurbanne-Northbound,Vourles-Northbound,Yzeron-Northboun")
        #self.ws.send("Lyon_8e_Arrondissement-Northbound")
        
        events.request_success.fire(
            request_type='WebSocket Sent',
            name='test/ws/connector',
            response_time=int((time.time() - start_at) * 1000000),
            response_length=len("Lyon_8e_Arrondissement-Northbound,Lyon_1er_Arrondissement-Northbound,Lyon_4e_Arrondissement-Northbound,Lyon_9e_Arrondissement-Northbound")
            #response_length=len("Lyon_8e_Arrondissement-Northbound")
        )

   

    
       
class WebsiteUser(HttpUser):

    tasks = [UserBehavior]
    wait_time = constant(10)