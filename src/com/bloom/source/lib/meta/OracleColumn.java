package com.bloom.source.lib.meta;

import com.bloom.source.lib.type.columntype;

public class OracleColumn
  extends DatabaseColumn
{
  public void setDataTypeName(String string)
  {
    this.dataTypeName = string;
    
    String str = this.dataTypeName;int i = -1;
    switch (str.hashCode())
    {
    case -472293131: 
      if (str.equals("VARCHAR2")) {
        i = 0;
      }
      break;
    case 66988604: 
      if (str.equals("FLOAT")) {
        i = 1;
      }
      break;
    case -1981034679: 
      if (str.equals("NUMBER")) {
        i = 2;
      }
      break;
    case 280179523: 
      if (str.equals("NVARCHAR2")) {
        i = 3;
      }
      break;
    case 2342524: 
      if (str.equals("LONG")) {
        i = 4;
      }
      break;
    case 2090926: 
      if (str.equals("DATE")) {
        i = 5;
      }
      break;
    case 80904: 
      if (str.equals("RAW")) {
        i = 6;
      }
      break;
    case -1292375964: 
      if (str.equals("LONG RAW")) {
        i = 7;
      }
      break;
    case 78168149: 
      if (str.equals("ROWID")) {
        i = 8;
      }
      break;
    case 2067286: 
      if (str.equals("CHAR")) {
        i = 9;
      }
      break;
    case 74101924: 
      if (str.equals("NCHAR")) {
        i = 10;
      }
      break;
    case -720779138: 
      if (str.equals("BINARY_FLOAT")) {
        i = 11;
      }
      break;
    case -923625009: 
      if (str.equals("BINARY_DOUBLE")) {
        i = 12;
      }
      break;
    case 74106186: 
      if (str.equals("NCLOB")) {
        i = 13;
      }
      break;
    case 2071548: 
      if (str.equals("CLOB")) {
        i = 14;
      }
      break;
    case 2041757: 
      if (str.equals("BLOB")) {
        i = 15;
      }
      break;
    case 63110334: 
      if (str.equals("BFILE")) {
        i = 16;
      }
      break;
    case -387696789: 
      if (str.equals("TIMESTAMP(0)")) {
        i = 17;
      }
      break;
    case -387696758: 
      if (str.equals("TIMESTAMP(1)")) {
        i = 18;
      }
      break;
    case -387696727: 
      if (str.equals("TIMESTAMP(2)")) {
        i = 19;
      }
      break;
    case -387696696: 
      if (str.equals("TIMESTAMP(3)")) {
        i = 20;
      }
      break;
    case -387696665: 
      if (str.equals("TIMESTAMP(4)")) {
        i = 21;
      }
      break;
    case -387696634: 
      if (str.equals("TIMESTAMP(5)")) {
        i = 22;
      }
      break;
    case -387696603: 
      if (str.equals("TIMESTAMP(6)")) {
        i = 23;
      }
      break;
    case -387696572: 
      if (str.equals("TIMESTAMP(7)")) {
        i = 24;
      }
      break;
    case -387696541: 
      if (str.equals("TIMESTAMP(8)")) {
        i = 25;
      }
      break;
    case -387696510: 
      if (str.equals("TIMESTAMP(9)")) {
        i = 26;
      }
      break;
    case -389516390: 
      if (str.equals("TIMESTAMP(0) WITH TIME ZONE")) {
        i = 27;
      }
      break;
    case 963793307: 
      if (str.equals("TIMESTAMP(1) WITH TIME ZONE")) {
        i = 28;
      }
      break;
    case -1977864292: 
      if (str.equals("TIMESTAMP(2) WITH TIME ZONE")) {
        i = 29;
      }
      break;
    case -624554595: 
      if (str.equals("TIMESTAMP(3) WITH TIME ZONE")) {
        i = 30;
      }
      break;
    case 728755102: 
      if (str.equals("TIMESTAMP(4) WITH TIME ZONE")) {
        i = 31;
      }
      break;
    case 2082064799: 
      if (str.equals("TIMESTAMP(5) WITH TIME ZONE")) {
        i = 32;
      }
      break;
    case -859592800: 
      if (str.equals("TIMESTAMP(6) WITH TIME ZONE")) {
        i = 33;
      }
      break;
    case 493716897: 
      if (str.equals("TIMESTAMP(7) WITH TIME ZONE")) {
        i = 34;
      }
      break;
    case 1847026594: 
      if (str.equals("TIMESTAMP(8) WITH TIME ZONE")) {
        i = 35;
      }
      break;
    case -1094631005: 
      if (str.equals("TIMESTAMP(9) WITH TIME ZONE")) {
        i = 36;
      }
      break;
    case 1690178642: 
      if (str.equals("INTERVAL YEAR(0) TO MONTH")) {
        i = 37;
      }
      break;
    case -106772717: 
      if (str.equals("INTERVAL YEAR(1) TO MONTH")) {
        i = 38;
      }
      break;
    case -1903724076: 
      if (str.equals("INTERVAL YEAR(2) TO MONTH")) {
        i = 39;
      }
      break;
    case 594291861: 
      if (str.equals("INTERVAL YEAR(3) TO MONTH")) {
        i = 40;
      }
      break;
    case -1202659498: 
      if (str.equals("INTERVAL YEAR(4) TO MONTH")) {
        i = 41;
      }
      break;
    case 1295356439: 
      if (str.equals("INTERVAL YEAR(5) TO MONTH")) {
        i = 42;
      }
      break;
    case -501594920: 
      if (str.equals("INTERVAL YEAR(6) TO MONTH")) {
        i = 43;
      }
      break;
    case 1996421017: 
      if (str.equals("INTERVAL YEAR(7) TO MONTH")) {
        i = 44;
      }
      break;
    case 199469658: 
      if (str.equals("INTERVAL YEAR(8) TO MONTH")) {
        i = 45;
      }
      break;
    case -1597481701: 
      if (str.equals("INTERVAL YEAR(9) TO MONTH")) {
        i = 46;
      }
      break;
    case -17138424: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(0)")) {
        i = 47;
      }
      break;
    case 1490413385: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(0)")) {
        i = 48;
      }
      break;
    case -1297002102: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(0)")) {
        i = 49;
      }
      break;
    case 210549707: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(0)")) {
        i = 50;
      }
      break;
    case 1718101516: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(0)")) {
        i = 51;
      }
      break;
    case -1069313971: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(0)")) {
        i = 52;
      }
      break;
    case 438237838: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(0)")) {
        i = 53;
      }
      break;
    case 1945789647: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(0)")) {
        i = 54;
      }
      break;
    case -841625840: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(0)")) {
        i = 55;
      }
      break;
    case 665925969: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(0)")) {
        i = 56;
      }
      break;
    case -17138393: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(1)")) {
        i = 57;
      }
      break;
    case 1490413416: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(1)")) {
        i = 58;
      }
      break;
    case -1297002071: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(1)")) {
        i = 59;
      }
      break;
    case 210549738: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(1)")) {
        i = 60;
      }
      break;
    case 1718101547: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(1)")) {
        i = 61;
      }
      break;
    case -1069313940: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(1)")) {
        i = 62;
      }
      break;
    case 438237869: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(1)")) {
        i = 63;
      }
      break;
    case 1945789678: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(1)")) {
        i = 64;
      }
      break;
    case -841625809: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(1)")) {
        i = 65;
      }
      break;
    case 665926000: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(1)")) {
        i = 66;
      }
      break;
    case -17138362: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(2)")) {
        i = 67;
      }
      break;
    case 1490413447: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(2)")) {
        i = 68;
      }
      break;
    case -1297002040: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(2)")) {
        i = 69;
      }
      break;
    case 210549769: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(2)")) {
        i = 70;
      }
      break;
    case 1718101578: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(2)")) {
        i = 71;
      }
      break;
    case -1069313909: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(2)")) {
        i = 72;
      }
      break;
    case 438237900: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(2)")) {
        i = 73;
      }
      break;
    case 1945789709: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(2)")) {
        i = 74;
      }
      break;
    case -841625778: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(2)")) {
        i = 75;
      }
      break;
    case 665926031: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(2)")) {
        i = 76;
      }
      break;
    case -17138331: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(3)")) {
        i = 77;
      }
      break;
    case 1490413478: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(3)")) {
        i = 78;
      }
      break;
    case -1297002009: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(3)")) {
        i = 79;
      }
      break;
    case 210549800: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(3)")) {
        i = 80;
      }
      break;
    case 1718101609: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(3)")) {
        i = 81;
      }
      break;
    case -1069313878: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(3)")) {
        i = 82;
      }
      break;
    case 438237931: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(3)")) {
        i = 83;
      }
      break;
    case 1945789740: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(3)")) {
        i = 84;
      }
      break;
    case -841625747: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(3)")) {
        i = 85;
      }
      break;
    case 665926062: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(3)")) {
        i = 86;
      }
      break;
    case -17138300: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(4)")) {
        i = 87;
      }
      break;
    case 1490413509: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(4)")) {
        i = 88;
      }
      break;
    case -1297001978: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(4)")) {
        i = 89;
      }
      break;
    case 210549831: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(4)")) {
        i = 90;
      }
      break;
    case 1718101640: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(4)")) {
        i = 91;
      }
      break;
    case -1069313847: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(4)")) {
        i = 92;
      }
      break;
    case 438237962: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(4)")) {
        i = 93;
      }
      break;
    case 1945789771: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(4)")) {
        i = 94;
      }
      break;
    case -841625716: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(4)")) {
        i = 95;
      }
      break;
    case 665926093: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(4)")) {
        i = 96;
      }
      break;
    case -17138269: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(5)")) {
        i = 97;
      }
      break;
    case 1490413540: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(5)")) {
        i = 98;
      }
      break;
    case -1297001947: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(5)")) {
        i = 99;
      }
      break;
    case 210549862: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(5)")) {
        i = 100;
      }
      break;
    case 1718101671: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(5)")) {
        i = 101;
      }
      break;
    case -1069313816: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(5)")) {
        i = 102;
      }
      break;
    case 438237993: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(5)")) {
        i = 103;
      }
      break;
    case 1945789802: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(5)")) {
        i = 104;
      }
      break;
    case -841625685: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(5)")) {
        i = 105;
      }
      break;
    case 665926124: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(5)")) {
        i = 106;
      }
      break;
    case -17138238: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(6)")) {
        i = 107;
      }
      break;
    case 1490413571: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(6)")) {
        i = 108;
      }
      break;
    case -1297001916: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(6)")) {
        i = 109;
      }
      break;
    case 210549893: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(6)")) {
        i = 110;
      }
      break;
    case 1718101702: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(6)")) {
        i = 111;
      }
      break;
    case -1069313785: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(6)")) {
        i = 112;
      }
      break;
    case 438238024: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(6)")) {
        i = 113;
      }
      break;
    case 1945789833: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(6)")) {
        i = 114;
      }
      break;
    case -841625654: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(6)")) {
        i = 115;
      }
      break;
    case 665926155: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(6)")) {
        i = 116;
      }
      break;
    case -17138207: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(7)")) {
        i = 117;
      }
      break;
    case 1490413602: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(7)")) {
        i = 118;
      }
      break;
    case -1297001885: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(7)")) {
        i = 119;
      }
      break;
    case 210549924: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(7)")) {
        i = 120;
      }
      break;
    case 1718101733: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(7)")) {
        i = 121;
      }
      break;
    case -1069313754: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(7)")) {
        i = 122;
      }
      break;
    case 438238055: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(7)")) {
        i = 123;
      }
      break;
    case 1945789864: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(7)")) {
        i = 124;
      }
      break;
    case -841625623: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(7)")) {
        i = 125;
      }
      break;
    case 665926186: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(7)")) {
        i = 126;
      }
      break;
    case -17138176: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(8)")) {
        i = 127;
      }
      break;
    case 1490413633: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(8)")) {
        i = 128;
      }
      break;
    case -1297001854: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(8)")) {
        i = 129;
      }
      break;
    case 210549955: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(8)")) {
        i = 130;
      }
      break;
    case 1718101764: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(8)")) {
        i = 131;
      }
      break;
    case -1069313723: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(8)")) {
        i = 132;
      }
      break;
    case 438238086: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(8)")) {
        i = 133;
      }
      break;
    case 1945789895: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(8)")) {
        i = 134;
      }
      break;
    case -841625592: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(8)")) {
        i = 135;
      }
      break;
    case 665926217: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(8)")) {
        i = 136;
      }
      break;
    case -17138145: 
      if (str.equals("INTERVAL DAY(0) TO SECOND(9)")) {
        i = 137;
      }
      break;
    case 1490413664: 
      if (str.equals("INTERVAL DAY(1) TO SECOND(9)")) {
        i = 138;
      }
      break;
    case -1297001823: 
      if (str.equals("INTERVAL DAY(2) TO SECOND(9)")) {
        i = 139;
      }
      break;
    case 210549986: 
      if (str.equals("INTERVAL DAY(3) TO SECOND(9)")) {
        i = 140;
      }
      break;
    case 1718101795: 
      if (str.equals("INTERVAL DAY(4) TO SECOND(9)")) {
        i = 141;
      }
      break;
    case -1069313692: 
      if (str.equals("INTERVAL DAY(5) TO SECOND(9)")) {
        i = 142;
      }
      break;
    case 438238117: 
      if (str.equals("INTERVAL DAY(6) TO SECOND(9)")) {
        i = 143;
      }
      break;
    case 1945789926: 
      if (str.equals("INTERVAL DAY(7) TO SECOND(9)")) {
        i = 144;
      }
      break;
    case -841625561: 
      if (str.equals("INTERVAL DAY(8) TO SECOND(9)")) {
        i = 145;
      }
      break;
    case 665926248: 
      if (str.equals("INTERVAL DAY(9) TO SECOND(9)")) {
        i = 146;
      }
      break;
    case -1783321312: 
      if (str.equals("UROWID")) {
        i = 147;
      }
      break;
    case -1177479995: 
      if (str.equals("TIMESTAMP(0) WITH LOCAL TIME ZONE")) {
        i = 148;
      }
      break;
    case 67284486: 
      if (str.equals("TIMESTAMP(1) WITH LOCAL TIME ZONE")) {
        i = 149;
      }
      break;
    case 1312048967: 
      if (str.equals("TIMESTAMP(2) WITH LOCAL TIME ZONE")) {
        i = 150;
      }
      break;
    case -1738153848: 
      if (str.equals("TIMESTAMP(3) WITH LOCAL TIME ZONE")) {
        i = 151;
      }
      break;
    case -493389367: 
      if (str.equals("TIMESTAMP(4) WITH LOCAL TIME ZONE")) {
        i = 152;
      }
      break;
    case 751375114: 
      if (str.equals("TIMESTAMP(5) WITH LOCAL TIME ZONE")) {
        i = 153;
      }
      break;
    case 1996139595: 
      if (str.equals("TIMESTAMP(6) WITH LOCAL TIME ZONE")) {
        i = 154;
      }
      break;
    case -1054063220: 
      if (str.equals("TIMESTAMP(7) WITH LOCAL TIME ZONE")) {
        i = 155;
      }
      break;
    case 190701261: 
      if (str.equals("TIMESTAMP(8) WITH LOCAL TIME ZONE")) {
        i = 156;
      }
      break;
    case 1435465742: 
      if (str.equals("TIMESTAMP(9) WITH LOCAL TIME ZONE")) {
        i = 157;
      }
      break;
    }
    switch (i)
    {
    case 0: 
      this.typeCode = 1;
      break;
    case 1: 
    case 2: 
      this.typeCode = 2;
      break;
    case 3: 
      this.typeCode = 3;
      break;
    case 4: 
      this.typeCode = 8;
      break;
    case 5: 
      this.typeCode = 12;
      break;
    case 6: 
      this.typeCode = 23;
      break;
    case 7: 
      this.typeCode = 24;
      break;
    case 8: 
      this.typeCode = 69;
      break;
    case 9: 
      this.typeCode = 96;
      break;
    case 10: 
      this.typeCode = 97;
      break;
    case 11: 
      this.typeCode = 100;
      break;
    case 12: 
      this.typeCode = 101;
      break;
    case 13: 
      this.typeCode = 111;
      break;
    case 14: 
      this.typeCode = 112;
      break;
    case 15: 
      this.typeCode = 113;
      break;
    case 16: 
      this.typeCode = 114;
      break;
    case 17: 
    case 18: 
    case 19: 
    case 20: 
    case 21: 
    case 22: 
    case 23: 
    case 24: 
    case 25: 
    case 26: 
      this.typeCode = 180;
      break;
    case 27: 
    case 28: 
    case 29: 
    case 30: 
    case 31: 
    case 32: 
    case 33: 
    case 34: 
    case 35: 
    case 36: 
      this.typeCode = 181;
      break;
    case 37: 
    case 38: 
    case 39: 
    case 40: 
    case 41: 
    case 42: 
    case 43: 
    case 44: 
    case 45: 
    case 46: 
      this.typeCode = 182;
      break;
    case 47: 
    case 48: 
    case 49: 
    case 50: 
    case 51: 
    case 52: 
    case 53: 
    case 54: 
    case 55: 
    case 56: 
    case 57: 
    case 58: 
    case 59: 
    case 60: 
    case 61: 
    case 62: 
    case 63: 
    case 64: 
    case 65: 
    case 66: 
    case 67: 
    case 68: 
    case 69: 
    case 70: 
    case 71: 
    case 72: 
    case 73: 
    case 74: 
    case 75: 
    case 76: 
    case 77: 
    case 78: 
    case 79: 
    case 80: 
    case 81: 
    case 82: 
    case 83: 
    case 84: 
    case 85: 
    case 86: 
    case 87: 
    case 88: 
    case 89: 
    case 90: 
    case 91: 
    case 92: 
    case 93: 
    case 94: 
    case 95: 
    case 96: 
    case 97: 
    case 98: 
    case 99: 
    case 100: 
    case 101: 
    case 102: 
    case 103: 
    case 104: 
    case 105: 
    case 106: 
    case 107: 
    case 108: 
    case 109: 
    case 110: 
    case 111: 
    case 112: 
    case 113: 
    case 114: 
    case 115: 
    case 116: 
    case 117: 
    case 118: 
    case 119: 
    case 120: 
    case 121: 
    case 122: 
    case 123: 
    case 124: 
    case 125: 
    case 126: 
    case 127: 
    case 128: 
    case 129: 
    case 130: 
    case 131: 
    case 132: 
    case 133: 
    case 134: 
    case 135: 
    case 136: 
    case 137: 
    case 138: 
    case 139: 
    case 140: 
    case 141: 
    case 142: 
    case 143: 
    case 144: 
    case 145: 
    case 146: 
      this.typeCode = 183;
      break;
    case 147: 
      this.typeCode = 208;
      break;
    case 148: 
    case 149: 
    case 150: 
    case 151: 
    case 152: 
    case 153: 
    case 154: 
    case 155: 
    case 156: 
    case 157: 
      this.typeCode = 231;
      break;
    default: 
      this.typeCode = 0;
    }
  }
  
  public void setDataType(int type)
  {
    this.typeCode = type;
    switch (this.typeCode)
    {
    case 1: 
      this.dataTypeName = "VARCHAR2";
      break;
    case 2: 
      this.dataTypeName = "NUMBER";
      break;
    case 3: 
      this.dataTypeName = "NVARCHAR2";
      break;
    case 8: 
      this.dataTypeName = "LONG";
      break;
    case 12: 
      this.dataTypeName = "DATE";
      break;
    case 23: 
      this.dataTypeName = "RAW";
      break;
    case 24: 
      this.dataTypeName = "LONG RAW";
      break;
    case 69: 
      this.dataTypeName = "ROWID";
      break;
    case 96: 
      this.dataTypeName = "CHAR";
      break;
    case 97: 
      this.dataTypeName = "NCHAR";
      break;
    case 100: 
      this.dataTypeName = "BINARY_FLOAT";
      break;
    case 101: 
      this.dataTypeName = "BINARY_DOUBLE";
      break;
    case 111: 
      this.dataTypeName = "NCLOB";
      break;
    case 112: 
      this.dataTypeName = "CLOB";
      break;
    case 113: 
      this.dataTypeName = "BLOB";
      break;
    case 114: 
      this.dataTypeName = "BFILE";
      break;
    case 180: 
      this.dataTypeName = "TIMESTAMP";
      break;
    case 181: 
      this.dataTypeName = "TIMESTAMP WITH TIME ZONE";
      break;
    case 182: 
      this.dataTypeName = "INTERVAL YEAR TO MONTH";
      break;
    case 183: 
      this.dataTypeName = "INTERVAL DAY TO SECOND";
      break;
    case 208: 
      this.dataTypeName = "UROWID";
      break;
    case 231: 
      this.dataTypeName = "TIMESTAMP WITH LOCAL TIME ZONE";
      break;
    default: 
      this.dataTypeName = "";
    }
  }
  
  public void setInternalColumnType(String dataTypeName)
  {
    if ((dataTypeName.equalsIgnoreCase("ROWID")) || (dataTypeName.equalsIgnoreCase("UROWID"))) {
      this.internalType = columntype.WA_STRING;
    }
    if ((dataTypeName.equalsIgnoreCase("NUMBER")) || (dataTypeName.equalsIgnoreCase("FLOAT"))) {
      this.internalType = columntype.WA_STRING;
    } else if (dataTypeName.equalsIgnoreCase("LONG")) {
      this.internalType = columntype.WA_STRING;
    } else if (dataTypeName.startsWith("INTERVAL DAY", 0)) {
      this.internalType = columntype.WA_STRING;
    } else if (dataTypeName.startsWith("INTERVAL YEAR", 0)) {
      this.internalType = columntype.WA_STRING;
    } else if ((dataTypeName.equalsIgnoreCase("NCHAR")) || (dataTypeName.equalsIgnoreCase("NVARCHAR2"))) {
      this.internalType = columntype.WA_UTF16_STRING;
    } else if ((dataTypeName.equalsIgnoreCase("CHAR")) || (dataTypeName.equalsIgnoreCase("VARCHAR2"))) {
      this.internalType = columntype.WA_STRING;
    } else if ((dataTypeName.equalsIgnoreCase("DATE")) || (dataTypeName.startsWith("TIMESTAMP", 0))) {
      this.internalType = columntype.WA_DATETIME;
    } else if (dataTypeName.equalsIgnoreCase("BINARY_FLOAT")) {
      this.internalType = columntype.WA_FLOAT;
    } else if (dataTypeName.equalsIgnoreCase("BINARY_DOUBLE")) {
      this.internalType = columntype.WA_DOUBLE;
    } else if ((dataTypeName.equalsIgnoreCase("RAW")) || (dataTypeName.equalsIgnoreCase("LONG RAW"))) {
      this.internalType = columntype.WA_STRING;
    } else if (dataTypeName.equalsIgnoreCase("BLOB")) {
      this.internalType = columntype.WA_STRING;
    } else if ((dataTypeName.equalsIgnoreCase("CLOB")) || (dataTypeName.equalsIgnoreCase("NCLOB"))) {
      this.internalType = columntype.WA_STRING;
    } else {
      this.internalType = columntype.WA_STRING;
    }
  }
}
