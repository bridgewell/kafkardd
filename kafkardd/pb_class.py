import bwproto.media.scupio.call_log_pb2
import bwproto.media.scupio.impression_log_pb2
import bwproto.media.scupio.click_log_pb2
import bwproto.media.scupio.conversion_log_pb2
import bwproto.rec.mall.implog_pb2
import bwproto.rec.mall.viewlog_pb2
import bwproto.rec.mall.clicklog_pb2
import bwproto.rec.mall.cartlog_pb2
import bwproto.rec.mall.buylog_pb2
import bwproto.rec.mall.loginlog_pb2
import bwproto.rec.mall.useractionlog_pb2
import bwproto.rec.exchange.doubleclick.rtblog_pb2
import bwproto.rec.exchange.doubleclick.rtblog_nobid_pb2
import bwproto.rec.exchange.scupio.scupiobidlog_pb2
import bwproto.rec.exchange.ucfunnel.ucfunnelbidinfo_pb2
import bwproto.rec.exchange.tanx.tanxbidlog_pb2
import bwproto.rec.exchange.adview.adview_pb2
import bwproto.rec.exchange.brightroll.brightroll_pb2
import bwproto.rec.learn.joinlog_pb2


class TOPIC:
    Scupio_Call =             'Scupio.Call'            
    Scupio_Impression =       'Scupio.Impression'      
    Scupio_Click =            'Scupio.Click'           
    Scupio_Conversion =       'Scupio.Conversion'      
    ECommerce_Impression =    'ECommerce.Impression'   
    ECommerce_View =          'ECommerce.View'         
    ECommerce_Click =         'ECommerce.Click'        
    ECommerce_Cart =          'ECommerce.Cart'         
    ECommerce_Buy =           'ECommerce.Buy'          
    ECommerce_Login =         'ECommerce.Login'        
    ECommerce_UserAction =    'ECommerce.UserAction'   
    CN_ECommerce_Impression = 'CN.ECommerce.Impression'
    CN_ECommerce_View =       'CN.ECommerce.View'      
    CN_ECommerce_Click =      'CN.ECommerce.Click'     
    CN_ECommerce_Cart =       'CN.ECommerce.Cart'      
    CN_ECommerce_Buy =        'CN.ECommerce.Buy'       
    CN_ECommerce_Login =      'CN.ECommerce.Login'     
    CN_ECommerce_UserAction = 'CN.ECommerce.UserAction'
    DSP_Google =              'DSP.Google'             
    DSP_Google_NoBid =        'DSP.Google.NoBid'       
    DSP_Scupio =              'DSP.Scupio'             
    DSP_ucfunnel =            'DSP.ucfunnel'           
    DSP_Tanx =                'DSP.Tanx'               
    DSP_AdView =              'DSP.AdView'             
    DSP_BrightRoll =          'DSP.BrightRoll'         
    Join_CTR =                'Join.CTR'


PB_CLASS = {
    TOPIC.Scupio_Call:              bwproto.media.scupio.call_log_pb2.CallLog,
    TOPIC.Scupio_Impression:        bwproto.media.scupio.impression_log_pb2.ImpressionLog,
    TOPIC.Scupio_Click:             bwproto.media.scupio.click_log_pb2.GeneralClickLog,
    TOPIC.Scupio_Conversion:        bwproto.media.scupio.conversion_log_pb2.ConversionLog,
    TOPIC.ECommerce_Impression:     bwproto.rec.mall.implog_pb2.ImpLog,
    TOPIC.ECommerce_View:           bwproto.rec.mall.viewlog_pb2.ViewLog,
    TOPIC.ECommerce_Click:          bwproto.rec.mall.clicklog_pb2.ClickLog,
    TOPIC.ECommerce_Cart:           bwproto.rec.mall.cartlog_pb2.CartLog,
    TOPIC.ECommerce_Buy:            bwproto.rec.mall.buylog_pb2.BuyLog,
    TOPIC.ECommerce_Login:          bwproto.rec.mall.loginlog_pb2.LoginLog,
    TOPIC.ECommerce_UserAction:     bwproto.rec.mall.useractionlog_pb2.UserActionLog,
    TOPIC.CN_ECommerce_Impression:  bwproto.rec.mall.implog_pb2.ImpLog,
    TOPIC.CN_ECommerce_View:        bwproto.rec.mall.viewlog_pb2.ViewLog,
    TOPIC.CN_ECommerce_Click:       bwproto.rec.mall.clicklog_pb2.ClickLog,
    TOPIC.CN_ECommerce_Cart:        bwproto.rec.mall.cartlog_pb2.CartLog,
    TOPIC.CN_ECommerce_Buy:         bwproto.rec.mall.buylog_pb2.BuyLog,
    TOPIC.CN_ECommerce_Login:       bwproto.rec.mall.loginlog_pb2.LoginLog,
    TOPIC.CN_ECommerce_UserAction:  bwproto.rec.mall.useractionlog_pb2.UserActionLog,
    TOPIC.DSP_Google:               bwproto.rec.exchange.doubleclick.rtblog_pb2.RTBLog,
    TOPIC.DSP_Google_NoBid:         bwproto.rec.exchange.doubleclick.rtblog_nobid_pb2.RTBNoBidLog,
    TOPIC.DSP_Scupio:               bwproto.rec.exchange.scupio.scupiobidlog_pb2.ScupioBidLog,
    TOPIC.DSP_ucfunnel:             bwproto.rec.exchange.ucfunnel.ucfunnelbidinfo_pb2.UcfunnelBidLog,
    TOPIC.DSP_Tanx:                 bwproto.rec.exchange.tanx.tanxbidlog_pb2.TanxBidLog,
    TOPIC.DSP_AdView:               bwproto.rec.exchange.adview.adview_pb2.AdViewBidLog,
    TOPIC.DSP_BrightRoll:           bwproto.rec.exchange.brightroll.brightroll_pb2.BrightRollBidLog,
    TOPIC.Join_CTR:                 bwproto.rec.learn.joinlog_pb2.CTRLog,
}
