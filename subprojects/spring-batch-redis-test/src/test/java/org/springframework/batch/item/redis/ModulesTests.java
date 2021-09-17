package org.springframework.batch.item.redis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.support.operation.JsonSet;
import org.springframework.batch.item.redis.test.Beer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ModulesTests extends AbstractTestBase {

    @Container
    protected static final RedisModulesContainer REDIS = new RedisModulesContainer();

    private RedisModulesClient client;
    private StatefulRedisModulesConnection<String, String> connection;

    @BeforeEach
    public void setup() {
        client = RedisModulesClient.create(REDIS.getRedisURI());
        connection = client.connect();
        connection.sync().flushall();
    }

    @AfterEach
    public void teardown() {
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
    }

    @Test
    public void testJsonSet() throws Exception {
        connection.sync().flushall();
        OperationItemWriter<String, String, JsonNode> writer = OperationItemWriter.client(client).operation(JsonSet.<String, JsonNode>key(n -> "beer:" + n.get("id").asText()).path(".").value(JsonNode::toString).build()).build();
        execute("json-set", Beer.jsonNodeReader(), writer);
        Assertions.assertEquals(18, connection.sync().keys("beer:*").size());
        String beer5UcMBc = "{\"id\":\"5UcMBc\",\"name\":\"\\\"Ignition\\\" IPA\",\"nameDisplay\":\"\\\"Ignition\\\" IPA\",\"description\":\"This medium amber beer is infused with a blend of 2-row malted barley and caramel malts. Its balance acquired by using a combination of Pacific Northwest hops for bitterness and aroma.\",\"abv\":\"6.6\",\"ibu\":\"45\",\"glasswareId\":5,\"availableId\":1,\"styleId\":30,\"isOrganic\":\"N\",\"isRetired\":\"N\",\"status\":\"verified\",\"statusDisplay\":\"Verified\",\"createDate\":\"2013-07-27 14:02:13\",\"updateDate\":\"2015-03-18 18:05:08\",\"glass\":{\"id\":5,\"name\":\"Pint\",\"createDate\":\"2012-01-03 02:41:33\"},\"available\":{\"id\":1,\"name\":\"Year Round\",\"description\":\"Available year round as a staple beer.\"},\"style\":{\"id\":30,\"categoryId\":3,\"category\":{\"id\":3,\"name\":\"North American Origin Ales\",\"createDate\":\"2012-03-21 20:06:45\"},\"name\":\"American-Style India Pale Ale\",\"shortName\":\"American IPA\",\"description\":\"American-style India pale ales are perceived to have medium-high to intense hop bitterness, flavor and aroma with medium-high alcohol content. The style is further characterized by floral, fruity, citrus-like, piney, resinous, or sulfur-like American-variety hop character. Note that one or more of these American-variety hop characters is the perceived end, but the hop characters may be a result of the skillful use of hops of other national origins. The use of water with high mineral content results in a crisp, dry beer. This pale gold to deep copper-colored ale has a full, flowery hop aroma and may have a strong hop flavor (in addition to the perception of hop bitterness). India pale ales possess medium maltiness which contributes to a medium body. Fruity-ester flavors and aromas are moderate to very strong. Diacetyl can be absent or may be perceived at very low levels. Chill and\\/or hop haze is allowable at cold temperatures. (English and citrus-like American hops are considered enough of a distinction justifying separate American-style IPA and English-style IPA categories or subcategories. Hops of other origins may be used for bitterness or approximating traditional American or English character. See English-style India Pale Ale\",\"ibuMin\":\"50\",\"ibuMax\":\"70\",\"abvMin\":\"6.3\",\"abvMax\":\"7.5\",\"srmMin\":\"6\",\"srmMax\":\"14\",\"ogMin\":\"1.06\",\"fgMin\":\"1.012\",\"fgMax\":\"1.018\",\"createDate\":\"2012-03-21 20:06:45\",\"updateDate\":\"2015-04-07 15:26:37\"},\"breweries\":[{\"id\":\"H8jawh\",\"name\":\"Working Man Brewing Company\",\"nameShortDisplay\":\"Working Man\",\"description\":\"Working Man Brewing is Livermore\\u00e2\\u0080\\u0099s newest brewery, located at 5542 Brisa Street, just off of Vasco Road.  Getting the brewery up and running has been a labor of love for the owners, taking the better part of 18 months to turn the space into a full production brewery.  We are excited to finally bring our love of beer and commitment to quality to Livermore, the Tri-Valley, and beyond.\\r\\n\\r\\nOur story is one of two independent breweries, Mt. Diablo Brewing and Working Man Brewing Company, coming together to work on one common goal.  Both companies were founded by award-winning home brewers who have a passion to bring high quality, well crafted beer that is fresh and local.  A chance meeting brought these working men together; a shared vision now combines forces under one name, Working Man Brewing Company.\\r\\n\\r\\nThe company\\u00e2\\u0080\\u0099s brewers have each have spent years working to perfect great beer recipes.  Beer geeks at heart, both will tinker and tweak the ingredients until the product is just right.  The main goal always being to make a beer they could be proud to share and proud to drink. We never let a beer go out the door that we don\\u00e2\\u0080\\u0099t believe in.\",\"website\":\"http:\\/\\/www.workingmanbrewing.com\\/\",\"established\":\"2013\",\"mailingListUrl\":\"http:\\/\\/beer.wickedcode.com\\/?page_id=641\",\"isOrganic\":\"N\",\"images\":{\"icon\":\"https:\\/\\/brewerydb-images.s3.amazonaws.com\\/brewery\\/H8jawh\\/upload_8EmbmI-icon.png\",\"medium\":\"https:\\/\\/brewerydb-images.s3.amazonaws.com\\/brewery\\/H8jawh\\/upload_8EmbmI-medium.png\",\"large\":\"https:\\/\\/brewerydb-images.s3.amazonaws.com\\/brewery\\/H8jawh\\/upload_8EmbmI-large.png\",\"squareMedium\":\"https:\\/\\/brewerydb-images.s3.amazonaws.com\\/brewery\\/H8jawh\\/upload_8EmbmI-squareMedium.png\",\"squareLarge\":\"https:\\/\\/brewerydb-images.s3.amazonaws.com\\/brewery\\/H8jawh\\/upload_8EmbmI-squareLarge.png\"},\"status\":\"verified\",\"statusDisplay\":\"Verified\",\"createDate\":\"2013-07-26 18:34:10\",\"updateDate\":\"2018-11-08 20:57:42\",\"isMassOwned\":\"N\",\"isInBusiness\":\"Y\",\"brewersAssociation\":{\"brewersAssocationId\":\"UJ82YQDSGH\",\"isCertifiedCraftBrewer\":\"Y\"},\"isVerified\":\"N\",\"locations\":[{\"id\":\"kDhJT9\",\"name\":\"Main Brewery\",\"streetAddress\":\"5542 Brisa Street\",\"extendedAddress\":\"Suite F\",\"locality\":\"Livermore\",\"region\":\"California\",\"postalCode\":\"94550\",\"phone\":\"(925) 269-9622\",\"website\":\"http:\\/\\/www.workingmanbrewing.com\\/\",\"hoursOfOperation\":\"Friday 4-7PM\\r\\nSaturday 12-6PM\\r\\nSunday 12-4PM\",\"latitude\":37.697526000000003,\"longitude\":-121.725189,\"isPrimary\":\"Y\",\"inPlanning\":\"N\",\"isClosed\":\"N\",\"openToPublic\":\"Y\",\"locationType\":\"micro\",\"locationTypeDisplay\":\"Micro Brewery\",\"countryIsoCode\":\"US\",\"yearOpened\":\"2013\",\"status\":\"verified\",\"statusDisplay\":\"Verified\",\"createDate\":\"2013-07-27 14:00:54\",\"updateDate\":\"2014-07-23 19:11:34\",\"hoursOfOperationNotes\":\"Friday 4-7PM\\r\\nSaturday 12-6PM\\r\\nSunday 12-4PM\",\"country\":{\"isoCode\":\"US\",\"name\":\"UNITED STATES\",\"displayName\":\"United States\",\"isoThree\":\"USA\",\"numberCode\":840,\"createDate\":\"2012-01-03 02:41:33\"}}]}]}";
        RedisJSONCommands<String, String> jsonCommands = connection.sync();
        Assertions.assertEquals(new ObjectMapper().readTree(beer5UcMBc), new ObjectMapper().readTree(jsonCommands.jsonGet("beer:5UcMBc")));
    }

}
