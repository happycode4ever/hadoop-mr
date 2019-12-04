package com.jj.hive.video.etl;

public class ETLUtil {
    private ETLUtil() {
    }

    //LzHjIj3fpR8	Xelanderthomas	686	Comedy	168	4545	4.58	273	167	udr9sLkoZ0s	3IU1GyX_zio	0E7Egr8Y1YI	qr8qZcvTLng	4WwVOWIqE80	Qeeq5OoLGJ0	YYDL1SqX-SY	vWGA5iYgAOU	8FeIj2HLN8k	bKlBTr88VTw	Y_59kWK5W3s	QlJSXVglZ3g	K3h_9O6OwW0	4ALe2z---e0	kdZk1Wk7kSw	hUa7f5XEzGE	aOihMldu_pE	PlPynB10vP0	W9DPlAZUH6Q	vta4RfQ2Z-I
    //SDNkMu8ZT68	w00dy911	630	People & Blogs	186	10181	3.49	494	257	rjnbgpPJUks
    public static String etl(String line) {
        StringBuffer sb = new StringBuffer();
        String[] fields = line.split("\t");
        //类别去空格
        fields[3] = fields[3].replace(" ", "");
        //可能存在丢失列，最后列允许没有
        if (fields.length < 9) {
            return null;
        } else {
            //最后列的相关id改用&拼接
            for (int i = 0; i < fields.length; i++) {
                sb.append(fields[i]);
                //最后列不拼字符
                if (i != fields.length - 1) {
                    //前8列或者列长10以上才拼第9列
                    if (i < 8 || (i == 8 && fields.length >= 10)) {
                        sb.append("\t");
                        //列长超过10从第10列开始拼&
                    } else if (i >= 9 && fields.length > 10) {
                        sb.append("&");
                    }
                }
            }


        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String line = "SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t3.49\t494\t257\t";
        System.out.println(etl(line));
    }
}
