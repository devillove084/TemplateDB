if(!self.define){let e,s={};const a=(a,f)=>(a=new URL(a+".js",f).href,s[a]||new Promise((s=>{if("document"in self){const e=document.createElement("script");e.src=a,e.onload=s,document.head.appendChild(e)}else e=a,importScripts(a),s()})).then((()=>{let e=s[a];if(!e)throw new Error(`Module ${a} didn’t register its module`);return e})));self.define=(f,i)=>{const d=e||("document"in self?document.currentScript.src:"")||location.href;if(s[d])return;let c={};const r=e=>a(e,d),b={module:{uri:d},exports:c,require:r};s[d]=Promise.all(f.map((e=>b[e]||r(e)))).then((e=>(i(...e),c)))}}define(["./workbox-53bdbe38"],(function(e){"use strict";e.setCacheNameDetails({prefix:"Docs Demo"}),self.addEventListener("message",(e=>{e.data&&"SKIP_WAITING"===e.data.type&&self.skipWaiting()})),e.clientsClaim(),e.precacheAndRoute([{url:"assets/_plugin-vue_export-helper.cdc0426e.js",revision:"25e3a5dcaf00fb2b1ba0c8ecea6d2560"},{url:"assets/404.html.11bd11a8.js",revision:"edd4a97ae135e9a974250876749ee809"},{url:"assets/404.html.5c976347.js",revision:"03bd028d153dd772d079380662f92291"},{url:"assets/app.4add0ceb.js",revision:"dcc155686732e26814c6402bd8c61158"},{url:"assets/auto.264f6c8c.js",revision:"1af7b89403d96b406782cd203249d58d"},{url:"assets/baz.html.62572b47.js",revision:"4e12489721256df02e0e07a6c65cb90b"},{url:"assets/baz.html.7a90ddf1.js",revision:"573b06e25c9c85b47e21081dbdcc6469"},{url:"assets/baz.html.d57ffabc.js",revision:"c1bc48de907538d251d9321aa5fb9bbb"},{url:"assets/baz.html.e44b8e0c.js",revision:"5b74e6255241f197df86f4eb01114348"},{url:"assets/disable.html.11fa7299.js",revision:"596aed38cb0e3ee3d085472d6b92321c"},{url:"assets/disable.html.28acf4fe.js",revision:"27a334a590c6432c60d4a34477ea4c36"},{url:"assets/disable.html.a8c22959.js",revision:"84ed60a47549e38f075db87a430b01ae"},{url:"assets/disable.html.db8f4d88.js",revision:"c841e44b770c397c10b713f4864e9b22"},{url:"assets/encrypt.html.4485a89b.js",revision:"b594ec6cf2615a62958ad1a62239e257"},{url:"assets/encrypt.html.8b32b0be.js",revision:"64ef3cb21e0a368f2e94ece3288c8786"},{url:"assets/encrypt.html.93077819.js",revision:"347fd9f894683b9bf58a72f011e3a809"},{url:"assets/encrypt.html.a3de3efc.js",revision:"c0a050dd5e573bfd9819fec02d214543"},{url:"assets/flowchart.parse.ee90d7e0.js",revision:"93ee4658efd463b82af7bc1b894a96d4"},{url:"assets/highlight.esm.d982e650.js",revision:"7755765e29eda27238d3160a257e85bd"},{url:"assets/index.29baef4d.js",revision:"3b44a831fed89fd1e293f6d7955dfd5d"},{url:"assets/index.html.01e09f26.js",revision:"f78f96ea7b95d5a5c60966880de33663"},{url:"assets/index.html.159ec5cb.js",revision:"964b0125af6c21f5d88ebc81296be327"},{url:"assets/index.html.1edbfc92.js",revision:"4a2a5f17bac2562144b75b5b5a5cf438"},{url:"assets/index.html.3b4da90a.js",revision:"fdea878097366e8c1cf7a258049aac42"},{url:"assets/index.html.52601f56.js",revision:"fcd0e751d7b8e392b0ca48c387b0b471"},{url:"assets/index.html.5caf921a.js",revision:"4e8cdc912857f7027500e4a87de8244c"},{url:"assets/index.html.6a85b12a.js",revision:"f796fb2c2d6a784b6475df6d909faca3"},{url:"assets/index.html.783dcd7e.js",revision:"e40273c3c053bf5fd3593f9a26424d72"},{url:"assets/index.html.7b3df199.js",revision:"47d50cdca1e323b559a6d7f3bb5c33f4"},{url:"assets/index.html.aa26cae5.js",revision:"a740d8f2ac1fa67579f1e927a816a7b7"},{url:"assets/index.html.abe48b0a.js",revision:"d6b06ddc1368b797faf19d3b8b728b0c"},{url:"assets/index.html.c05fc3cc.js",revision:"6f66c1c5e57137887abc2b3e5fc6a184"},{url:"assets/index.html.ca65c214.js",revision:"487150d79f38dddacbe7ad5a39e21593"},{url:"assets/index.html.ce97bca0.js",revision:"7300b813bbff1946f1e8be6a10d2f861"},{url:"assets/index.html.d33f8604.js",revision:"c3dd9f022a25665ddee46d63f5e06cab"},{url:"assets/index.html.df25d03f.js",revision:"10f57587429ae27f86d4ee42c8381f34"},{url:"assets/index.html.e064a8f3.js",revision:"667152537216e861451d128e54935b23"},{url:"assets/index.html.f35a97d5.js",revision:"7f82dd1c7613ec63fc7d1055fcab02a2"},{url:"assets/index.html.f9528cd4.js",revision:"f081f8a328294a4ae0c127607c5bc69e"},{url:"assets/index.html.ff7b0334.js",revision:"3b4dee74f77853a3284a3a467b070fb1"},{url:"assets/KaTeX_AMS-Regular.0cdd387c.woff2",revision:"66c678209ce93b6e2b583f02ce41529e"},{url:"assets/KaTeX_AMS-Regular.30da91e8.woff",revision:"10824af77e9961cfd548c8a458f10851"},{url:"assets/KaTeX_AMS-Regular.68534840.ttf",revision:"56573229753fad48910bda2ea1a6dd54"},{url:"assets/KaTeX_Caligraphic-Bold.07d8e303.ttf",revision:"497bf407c4c609c6cf1f1ad38f437f7f"},{url:"assets/KaTeX_Caligraphic-Bold.1ae6bd74.woff",revision:"de2ba279933d60f7819ff61f71c17bed"},{url:"assets/KaTeX_Caligraphic-Bold.de7701e4.woff2",revision:"a9e9b0953b078cd40f5e19ef4face6fc"},{url:"assets/KaTeX_Caligraphic-Regular.3398dd02.woff",revision:"a25140fbe6692bffe71a2ab861572eb3"},{url:"assets/KaTeX_Caligraphic-Regular.5d53e70a.woff2",revision:"08d95d99bf4a2b2dc7a876653857f154"},{url:"assets/KaTeX_Caligraphic-Regular.ed0b7437.ttf",revision:"e6fb499fc8f9925eea3138cccba17fff"},{url:"assets/KaTeX_Fraktur-Bold.74444efd.woff2",revision:"796f3797cdf36fcaea18c3070a608378"},{url:"assets/KaTeX_Fraktur-Bold.9163df9c.ttf",revision:"b9d7c4497cab3702487214651ab03744"},{url:"assets/KaTeX_Fraktur-Bold.9be7ceb8.woff",revision:"40934fc076960bb989d590db044fef62"},{url:"assets/KaTeX_Fraktur-Regular.1e6f9579.ttf",revision:"97a699d83318e9334a0deaea6ae5eda2"},{url:"assets/KaTeX_Fraktur-Regular.51814d27.woff2",revision:"f9e6a99f4a543b7d6cad1efb6cf1e4b1"},{url:"assets/KaTeX_Fraktur-Regular.5e28753b.woff",revision:"e435cda5784e21b26ab2d03fbcb56a99"},{url:"assets/KaTeX_Main-Bold.0f60d1b8.woff2",revision:"a9382e25bcf75d856718fcef54d7acdb"},{url:"assets/KaTeX_Main-Bold.138ac28d.ttf",revision:"8e431f7ece346b6282dae3d9d0e7a970"},{url:"assets/KaTeX_Main-Bold.c76c5d69.woff",revision:"4cdba6465ab9fac5d3833c6cdba7a8c3"},{url:"assets/KaTeX_Main-BoldItalic.70ee1f64.ttf",revision:"52fb39b0434c463d5df32419608ab08a"},{url:"assets/KaTeX_Main-BoldItalic.99cd42a3.woff2",revision:"d873734390c716d6e18ff3f71ac6eb8b"},{url:"assets/KaTeX_Main-BoldItalic.a6f7ec0d.woff",revision:"5f875f986a9bce1264e8c42417b56f74"},{url:"assets/KaTeX_Main-Italic.0d85ae7c.ttf",revision:"39349e0a2b366f38e2672b45aded2030"},{url:"assets/KaTeX_Main-Italic.97479ca6.woff2",revision:"652970624cde999882102fa2b6a8871f"},{url:"assets/KaTeX_Main-Italic.f1d6ef86.woff",revision:"8ffd28f6390231548ead99d7835887fa"},{url:"assets/KaTeX_Main-Regular.c2342cd8.woff2",revision:"f8a7f19f45060f7a177314855b8c7aa3"},{url:"assets/KaTeX_Main-Regular.c6368d87.woff",revision:"f1cdb692ee31c10b37262caffced5271"},{url:"assets/KaTeX_Main-Regular.d0332f52.ttf",revision:"818582dae57e6fac46202cfd844afabb"},{url:"assets/KaTeX_Math-BoldItalic.850c0af5.woff",revision:"48155e43d9a284b54753e50e4ba586dc"},{url:"assets/KaTeX_Math-BoldItalic.dc47344d.woff2",revision:"1320454d951ec809a7dbccb4f23fccf0"},{url:"assets/KaTeX_Math-BoldItalic.f9377ab0.ttf",revision:"6589c4f1f587f73f0ad0af8ae35ccb53"},{url:"assets/KaTeX_Math-Italic.08ce98e5.ttf",revision:"fe5ed5875d95b18c98546cb4f47304ff"},{url:"assets/KaTeX_Math-Italic.7af58c5e.woff2",revision:"d8b7a801bd87b324efcbae7394119c24"},{url:"assets/KaTeX_Math-Italic.8a8d2445.woff",revision:"ed7aea12d765f9e2d0f9bc7fa2be626c"},{url:"assets/KaTeX_SansSerif-Bold.1ece03f7.ttf",revision:"f2ac73121357210d91e5c3eaa42f72ea"},{url:"assets/KaTeX_SansSerif-Bold.e99ae511.woff2",revision:"ad546b4719bcf690a3604944b90b7e42"},{url:"assets/KaTeX_SansSerif-Bold.ece03cfd.woff",revision:"0e897d27f063facef504667290e408bd"},{url:"assets/KaTeX_SansSerif-Italic.00b26ac8.woff2",revision:"e934cbc86e2d59ceaf04102c43dc0b50"},{url:"assets/KaTeX_SansSerif-Italic.3931dd81.ttf",revision:"f60b4a34842bb524b562df092917a542"},{url:"assets/KaTeX_SansSerif-Italic.91ee6750.woff",revision:"ef725de572b71381dccf53918e300744"},{url:"assets/KaTeX_SansSerif-Regular.11e4dc8a.woff",revision:"5f8637ee731482c44a37789723f5e499"},{url:"assets/KaTeX_SansSerif-Regular.68e8c73e.woff2",revision:"1ac3ed6ebe34e473519ca1da86f7a384"},{url:"assets/KaTeX_SansSerif-Regular.f36ea897.ttf",revision:"3243452ee6817acd761c9757aef93c29"},{url:"assets/KaTeX_Script-Regular.036d4e95.woff2",revision:"1b3161eb8cc67462d6e8c2fb96c68507"},{url:"assets/KaTeX_Script-Regular.1c67f068.ttf",revision:"a189c37d73ffce63464635dc12cbbc96"},{url:"assets/KaTeX_Script-Regular.d96cdf2b.woff",revision:"a82fa2a7e18b8c7a1a9f6069844ebfb9"},{url:"assets/KaTeX_Size1-Regular.6b47c401.woff2",revision:"82ef26dc680ba60d884e051c73d9a42d"},{url:"assets/KaTeX_Size1-Regular.95b6d2f1.ttf",revision:"0d8d9204004bdf126342605f7bbdffe6"},{url:"assets/KaTeX_Size1-Regular.c943cc98.woff",revision:"4788ba5b6247e336f734b742fe9900d5"},{url:"assets/KaTeX_Size2-Regular.2014c523.woff",revision:"b0628bfd27c979a09f702a2277979888"},{url:"assets/KaTeX_Size2-Regular.a6b2099f.ttf",revision:"1fdda0e59ed35495ebac28badf210574"},{url:"assets/KaTeX_Size2-Regular.d04c5421.woff2",revision:"95a1da914c20455a07b7c9e2dcf2836d"},{url:"assets/KaTeX_Size3-Regular.500e04d5.ttf",revision:"963af864cbb10611ba33267ba7953777"},{url:"assets/KaTeX_Size3-Regular.6ab6b62e.woff",revision:"4de844d4552e941f6b9c38837a8d487b"},{url:"assets/KaTeX_Size4-Regular.99f9c675.woff",revision:"3045a61f722bc4b198450ce69b3e3824"},{url:"assets/KaTeX_Size4-Regular.a4af7d41.woff2",revision:"61522cd3d9043622e235ab57762754f2"},{url:"assets/KaTeX_Size4-Regular.c647367d.ttf",revision:"27a23ee69999affa55491c7dab8e53bf"},{url:"assets/KaTeX_Typewriter-Regular.71d517d6.woff2",revision:"b8b8393d2e65fcebda5fa99fa3264f41"},{url:"assets/KaTeX_Typewriter-Regular.e14fed02.woff",revision:"0e0460587676d22eae09accd6dcfebc6"},{url:"assets/KaTeX_Typewriter-Regular.f01f3e87.ttf",revision:"6bf4287568e1d3004b54d5d60f9f08f9"},{url:"assets/league-gothic.38fcc721.ttf",revision:"91295fa87df918411b49b7531da5d558"},{url:"assets/league-gothic.5eef6df8.woff",revision:"cd382dc8a9d6317864b5810a320effc5"},{url:"assets/league-gothic.8802c66a.eot",revision:"9900a4643cc63c5d8f969d2196f72572"},{url:"assets/markdown.esm.832a189d.js",revision:"0d05be8d1ccc17a6f2270457575848a1"},{url:"assets/markdown.html.2286601f.js",revision:"ac59dcf84a4fca373da1e23d64beb8d4"},{url:"assets/markdown.html.929124fd.js",revision:"d2ca0b0f32fbfcbceb2fab7fd8c12e50"},{url:"assets/markdown.html.99b8c24c.js",revision:"639295608e766e24ccba06d64fe5b358"},{url:"assets/markdown.html.cd21eb91.js",revision:"857b36e4881efb47f664264d25db5657"},{url:"assets/math.esm.a3f84b6f.js",revision:"e77d289bc577da4e7341dc5a62209df1"},{url:"assets/mermaid.esm.min.e3b5d21d.js",revision:"481e9564c28a71aed6f3c286b4911f29"},{url:"assets/notes.esm.3c361cb7.js",revision:"b055b0fe912d3e63c622ee92caf08028"},{url:"assets/page.html.54fa3019.js",revision:"00d38ab48c5a54c2a670639a746b5a15"},{url:"assets/page.html.62e3d4b7.js",revision:"37e752e59e76592f2f1d89effa92fa79"},{url:"assets/page.html.c3b0d027.js",revision:"4932a03bde7a783fa300034c53c47ae9"},{url:"assets/page.html.e6491595.js",revision:"0c989a60b8b5135fef42151af17cd3af"},{url:"assets/photoswipe.esm.382b1873.js",revision:"58c8e5a3e1981882b36217b62f1c7bae"},{url:"assets/ray.html.2ba27a2e.js",revision:"614dab698ecb27cfdde3fb36f6fc1adf"},{url:"assets/ray.html.631a153f.js",revision:"c6f427842407deb3e527a877ec120ddd"},{url:"assets/ray.html.df8c435e.js",revision:"aa02b9cc544654a6455c526608824065"},{url:"assets/ray.html.f3dd4cd3.js",revision:"a59e3f1a050ec77be21148c9a0024ba6"},{url:"assets/reveal.esm.b96f05d8.js",revision:"40ef902ff74efca41d50e4c94edb2b83"},{url:"assets/search.esm.80da4a02.js",revision:"7d8008309758cac57a4dd66a633819ba"},{url:"assets/slides.html.2916d7f5.js",revision:"5f4ecd2121b17978c36a38fb369c350a"},{url:"assets/slides.html.7d16ecef.js",revision:"ba45d1a66d95dd8d4532b9329f3a9de7"},{url:"assets/slides.html.8733d74a.js",revision:"3969f370efcab8af02c413637588c751"},{url:"assets/slides.html.e276b5ee.js",revision:"4bd8e695b18e9a12b674ea640ac878fe"},{url:"assets/source-sans-pro-italic.05d3615f.woff",revision:"e74f0128884561828ce8c9cf5c284ab8"},{url:"assets/source-sans-pro-italic.ad4b0799.eot",revision:"72217712eb8d28872e7069322f3fda23"},{url:"assets/source-sans-pro-italic.d13268af.ttf",revision:"8256cfd7e4017a7690814879409212cd"},{url:"assets/source-sans-pro-regular.c1865d89.ttf",revision:"2da39ecf9246383937da11b44b7bd9b4"},{url:"assets/source-sans-pro-regular.d4eaa48b.woff",revision:"e7acc589bb558fe58936a853f570193c"},{url:"assets/source-sans-pro-regular.dce8869d.eot",revision:"1d71438462d532b62b05cdd7e6d7197d"},{url:"assets/source-sans-pro-semibold.a53e2723.ttf",revision:"f3565095e6c9158140444970f5a2c5ed"},{url:"assets/source-sans-pro-semibold.b0abd273.woff",revision:"1cb8e94f1185f1131a0c895165998f2b"},{url:"assets/source-sans-pro-semibold.ebb8918d.eot",revision:"0f3da1edf1b5c6a94a6ad948a7664451"},{url:"assets/source-sans-pro-semibolditalic.7225cacc.woff",revision:"6b058fc2634b01d837c3432316c3141f"},{url:"assets/source-sans-pro-semibolditalic.dfe0b47a.eot",revision:"58153ac7194e141d1e73ea88c6b63861"},{url:"assets/source-sans-pro-semibolditalic.e8ec22b6.ttf",revision:"c7e698a4d0956f4a939f42a05685bbf5"},{url:"assets/style.85a59f69.css",revision:"e42ade91f62f15043f46e418c55297de"},{url:"assets/vue-repl.ac0337c1.js",revision:"87c409e5ebeb7fbf14aee58130acd81d"},{url:"assets/VuePlayground.e69f9613.js",revision:"fab085bc0f181be7726778f8533b88a9"},{url:"assets/waline-meta.8c8f8f9e.js",revision:"614e9510b9a7a9c2164ae5827a7649f4"},{url:"assets/zoom.esm.8514a202.js",revision:"8b3ee4f6f71ef2a7c85901cba6d23344"},{url:"logo.svg",revision:"1a8e6bd1f66927a7dcfeb4b22f33ffde"},{url:"404.html",revision:"23c87c3336c4ed719f527307e0e1bda7"},{url:"demo/disable.html",revision:"7f03916c04e4e3f69eeeadf99545f0db"},{url:"demo/encrypt.html",revision:"94d3392d6d9fe978ae4156a15aea48b9"},{url:"demo/index.html",revision:"edb82545cfaf3b8c1ac6955489ebb0a5"},{url:"demo/markdown.html",revision:"876daffa9b0e25eee8b3d6faf9664fe3"},{url:"demo/page.html",revision:"67f09eb006ee034c719d75415df6c389"},{url:"guide/bar/baz.html",revision:"4d7d46c059af59bd0e2ed0010f7c8f77"},{url:"guide/bar/index.html",revision:"e53708b1358f333ecfcb422ccb75e533"},{url:"guide/foo/index.html",revision:"e2436ecb2536bc443e68ba012e40ecd9"},{url:"guide/foo/ray.html",revision:"57b7dc5630b2999603c75b123e43efc8"},{url:"guide/index.html",revision:"c3445506616e9a736d087dd587ba2b7d"},{url:"index.html",revision:"f1cd8ddf434371ce2448698ac8ba0b62"},{url:"slides.html",revision:"20a10ff75ac75578033cc1aee2e2d298"},{url:"zh/demo/disable.html",revision:"a56b093fd8b94adb3182da3300e70fb7"},{url:"zh/demo/encrypt.html",revision:"2fde2cd11733731173c6f2985f5f81f3"},{url:"zh/demo/index.html",revision:"19b0fccd6bded5d470652ffa7b2b43fa"},{url:"zh/demo/markdown.html",revision:"f602de72dd10cf96aeb0c393fdab56ff"},{url:"zh/demo/page.html",revision:"405437481d35bd15b08c808606a1ecc1"},{url:"zh/guide/bar/baz.html",revision:"0a53de3ee7e0eb3d4feb94cf2de05c02"},{url:"zh/guide/bar/index.html",revision:"47d6f9efe75455b78e7ef70744099d39"},{url:"zh/guide/foo/index.html",revision:"4fc6c6b52071a2fa6fcf251367fbc19e"},{url:"zh/guide/foo/ray.html",revision:"b5b2289df77c2e08c0389c3e8cd72c43"},{url:"zh/guide/index.html",revision:"2c771f7b06cfcf196b20b8eeccd0fa10"},{url:"zh/index.html",revision:"e1bf25769db99114ea42ca3a9bb326e1"},{url:"zh/slides.html",revision:"0645077c860ce9209986a574676e1cf8"},{url:"assets/icon/apple-icon-152.png",revision:"8b700cd6ab3f7ff38a82ee491bf3c994"},{url:"assets/icon/chrome-192.png",revision:"6d4cd350c650faaed8da00eb05a2a966"},{url:"assets/icon/chrome-512.png",revision:"b08fe93ce982da9d3b0c7e74e0c4e359"},{url:"assets/icon/chrome-mask-192.png",revision:"a05b03eeb7b69dc96355f36f0766b310"},{url:"assets/icon/chrome-mask-512.png",revision:"3c4d57a60277792c6c005494657e1dce"},{url:"assets/icon/guide-maskable.png",revision:"99cc77cf2bc792acd6b847b5e3e151e9"},{url:"assets/icon/guide-monochrome.png",revision:"699fa9b069f7f09ce3d52be1290ede20"},{url:"assets/icon/ms-icon-144.png",revision:"2fe199405e0366e50ac0442cc4f33a34"},{url:"logo.png",revision:"b1cc915c4cbb67972e27267862bcd80a"}],{}),e.cleanupOutdatedCaches()}));
//# sourceMappingURL=service-worker.js.map
